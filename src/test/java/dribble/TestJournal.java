package dribble;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class TestJournal {
    
    private static final byte[] SLAB = new byte[100 * 0x00100000];
    private static final Random random = new Random(System.nanoTime());
    
    private final TestParams params;
    private File dir;
    private Journal journal;
    private static final int SEGMENT_SIZE = 0x00100000; // 1mb
    
    @BeforeClass
    public static void fillSlab() {
        random.nextBytes(SLAB);
    }
    
    private static ByteBuffer bytes(int size) {
        if (size > SLAB.length)
            throw new RuntimeException("Can't have that many bytes");
        return ByteBuffer.wrap(SLAB, 0, size);
    }
    
    private static void blockUntilNoPendingWrites(Journal journal, int maxWait) throws TimeoutException {
        long start = System.currentTimeMillis();
        while (journal.getBacklogSize() > 0) {
            try { Thread.currentThread().sleep(50); } catch (Exception ex) {}
            long now = System.currentTimeMillis();
            if (now - start > maxWait) {
                throw new TimeoutException("Pending writes timeout expired");
            }
        }
    }
    
    private static Journal buildJournal(File dir) {
        SegmentFactory segmentFactory = FileSegmentFactory.FileSegmentFactoryBuilder
                .newBuilder()
                .withBytesBetweenSync(0x00080000)
                .withMillisBetweenSync(1000)
                .withSyncAfterEveryAppend(false)
                .withWritesBetweenSync(100)
                .withDirectory(dir)
                .build();
        
        return new Journal(segmentFactory, SEGMENT_SIZE);
    }
    
    public TestJournal(TestParams params) {
        this.params = params;
    }
    
    @Before
    public void setUpJournal() {
        dir = TestUtil.getRandomTempDir();
        journal = buildJournal(dir);
    }
    
    // make sure everything is in working order.
    @Test
    public void write100Megs() throws Exception {
        final int segmentSize = 0x00100000;
        final int expectedSegments = 100;
        
        for (int i = 0; i < expectedSegments; i++) {
            journal.append(bytes(segmentSize));
        }
        journal.drain();
        
        Assert.assertEquals(expectedSegments * 2, dir.list().length);
    }
    
    @Test
    public void testMark() throws Exception {
        final int writes = (params.journalSizeInMb * 0x00100000) / params.bufSize;
        
        // expected recoveries falls apart if this is not the case.
        Assert.assertTrue(writes % 2 == 0);
        
        ArrayList<Journal.Receipt> receipts = new ArrayList<Journal.Receipt>();
        
        for (int i = 0; i < writes; i++) {
            Journal.Receipt receipt = journal.append(bytes(params.bufSize));
            receipts.add(receipt);
        }
        
        // mark the middle+1 one.
        journal.mark(receipts.get(writes / 2));
        journal.drain();
        journal.close();
        Assert.assertEquals(writes, journal.getAppendsSinceOpen());
        
        // rebuild the journal.
        journal = buildJournal(dir);
        Assert.assertEquals(0, journal.getAppendsSinceOpen());

        final AtomicInteger recoveries = new AtomicInteger(0);
        JournalObserver observer = new JournalObserver() {
            public void recover(ByteBuffer buf) {
                recoveries.incrementAndGet();
            }
        };
        journal.recover(observer);
        
        Assert.assertEquals(writes / 2 - 1, recoveries.get());
        
    }
        
    @After
    public void closeJournal() throws IOException {
        journal.close();
    }
    
    @After
    public void disposeJournal() {
        TestUtil.removeDir(dir);
    }
    
    private static class TestParams {
        private final int bufSize;
        private final int journalSizeInMb;
        
        public TestParams(int bufSize, int journalSizeInMb) {
            this.bufSize = bufSize;
            this.journalSizeInMb = journalSizeInMb;
        }
    }
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return new ArrayList<Object[]>() {{
            add(new Object[] { new TestParams(1024, 10)});
            add(new Object[] { new TestParams(1024, 100)});
            add(new Object[] { new TestParams(100, 100)}); 
            
        }};
    }
}
