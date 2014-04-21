package dribble;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class ConcurrentTestJournalSettings {
    private static final Random random = new Random(System.nanoTime());
    
    private final TestParams params;
    
    public ConcurrentTestJournalSettings(TestParams params) {
        this.params = params;
    }
    
    @Test
    public void testJournal() throws Exception {
        
        final int bytesToWrite = 0x00100000 * params.megabytes;
        
        final ArrayList<Exception> errors = new ArrayList<Exception>();
        final byte[] buf = new byte[params.bufferSize];
        random.nextBytes(buf);
        
        File dir = TestUtil.getRandomTempDir();
        System.out.println(dir.getAbsolutePath());
        
        SegmentFactory factory = FileSegmentFactory.FileSegmentFactoryBuilder.newBuilder()
                .withDirectory(dir)
                .withBytesBetweenSync(0x00100000)
                .withMillisBetweenSync(1000)
                .withWritesBetweenSync(10)
                .withSyncAfterEveryAppend(false)
                .build();
        final Journal journal = new Journal(factory, 0x00100000 * 10);
        
        final CountDownLatch startLatch = new CountDownLatch(params.numThreads);
        final CountDownLatch waitLatch = new CountDownLatch(params.numThreads);
        
        Runnable writeRunnable = new Runnable() {
            public void run() {
                startLatch.countDown();
                try {
                    startLatch.await();
                } catch (Exception ex) {
                    errors.add(ex);
                    return;
                }
                int thisThreadWillWrite = bytesToWrite / params.numThreads;
                int written = 0;
                try {
                    while (written < thisThreadWillWrite) {
                        Journal.Receipt receipt = journal.append(ByteBuffer.wrap(buf));
                        if (random.nextFloat() < params.markChance) {
                            journal.mark(receipt);
                        }
                        written += buf.length;
                    }
                } catch (IOException ex) {
                    errors.add(ex);
                } finally {
                    waitLatch.countDown();
                }
            }
        };

        long start = System.currentTimeMillis();
        for (int i = 0; i < params.numThreads; i++) {
            new Thread(writeRunnable, "writer-" + i).start();
        }
        
        waitLatch.await(30, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        
        journal.drain();
        long realEnd = System.currentTimeMillis();
        
        if (errors.size() > 0) {
            for (Exception ex : errors) {
                ex.printStackTrace();
            }
            Assert.fail("Check log for errors");
        }
        
        Assert.assertEquals(11 * 2, dir.list().length);
        TestUtil.removeDir(dir);
        
        System.out.println(String.format("total/append: %d/%d", realEnd-start, end-start));
    }
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return new ArrayList<Object[]>() {{
            add(new Object[] { new TestParams(2, 1024, 100, 0.0f)});
            add(new Object[] { new TestParams(3, 1024, 100, 0.0001f)});
            add(new Object[] { new TestParams(4, 1024, 100, 0.01f)});
            add(new Object[] { new TestParams(5, 1024, 100, 0.25f)});
        }};
    }
    
    private static class TestParams {
        final int numThreads;
        final int bufferSize;
        final int megabytes;
        final float markChance;
        
        public TestParams(int numThreads, int bufferSize, int megabytes, float markChance) {
            this.numThreads = numThreads;
            this.bufferSize = bufferSize;
            this.megabytes = megabytes;
            this.markChance = markChance;
        }
    }
}
