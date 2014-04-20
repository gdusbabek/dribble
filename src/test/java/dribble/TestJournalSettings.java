package dribble;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class TestJournalSettings {
    private static final Random random = new Random(System.nanoTime());
    
    private final TestParams params;
    
    public TestJournalSettings(TestParams params) {
        this.params = params;    
    }
    
    @Test
    public void testWriting() throws Exception {
        byte[] bigBuf = new byte[params.bufferSize];
        random.nextBytes(bigBuf);
        
        File dir = Util.getRandomTempDir();
        System.out.println(dir.getAbsolutePath());
        
        SegmentFactory factory = FileSegmentFactory.FileSegmentFactoryBuilder.newBuilder()
                .withDirectory(dir)
                .withBytesBetweenSync(0x00100000)
                .withMillisBetweenSync(1000)
                .withWritesBetweenSync(10)
                .withSyncAfterEveryAppend(false)
                .build();
        Journal journal = new Journal(factory, 0x00100000 * 10);
        
        long bytesWritten = 0;
        long writes = 0;
        long marks = 0;
        long a = System.currentTimeMillis();
        while (bytesWritten < params.megabytes * 0x00100000) {
            Journal.Receipt r = journal.append(ByteBuffer.wrap(bigBuf));
            if (random.nextFloat() < params.markChance) {
                journal.mark(r);
                marks += 1;
            }
            bytesWritten += bigBuf.length;
            writes += 1;
        }
        long b = System.currentTimeMillis();
        journal.drain();
        long c = System.currentTimeMillis();
        journal.close();
        long d = System.currentTimeMillis();
        
        System.out.println(String.format("total time    : %d", d-a));
        System.out.println(String.format("append        : %d", b-a));
        System.out.println(String.format("draining      : %d", c-b));
        System.out.println(String.format("closing       : %d", d-c));
        System.out.println(String.format("writes,size   : %d, %d", writes, params.bufferSize));
        System.out.println(String.format("marks         : %d", marks));
        System.out.println();
              
        Assert.assertEquals(11 * 2, dir.list().length);
        Util.removeDir(dir);
    }
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return new ArrayList<Object[]>() {{
            add(new Object[] { new TestParams(1024, 100, 0.0f)});
            add(new Object[] { new TestParams(1024, 100, 0.0001f)});
            add(new Object[] { new TestParams(1024, 100, 0.01f)});
            add(new Object[] { new TestParams(1024, 100, 0.25f)});
        }};
    }
    
    private static class TestParams {
        private final int bufferSize;
        private final int megabytes;
        private final float markChance;
        
        private TestParams(int bufferSize, int megabytes, float markChance) {
            this.bufferSize = bufferSize;
            this.megabytes = megabytes;
            this.markChance = markChance;
        }
    }
}
