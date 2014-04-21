package dribble;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class TestSegment {
    private static final Random random = new Random(System.nanoTime());
    
    @Test
    public void testUncontestedWriteSpeed() throws IOException {
        byte[] bigBuf = new byte[0x00100000];
        random.nextBytes(bigBuf);
        
        File dir = TestUtil.getRandomTempDir();
        System.out.println(dir.getAbsolutePath());
        File data = new File(dir, "segment.log");
        File meta = new File(dir, "segment.meta");
        
        Segment segment = Segment.forWrite(new FileDataIO(data, 0), new FileMetaIO(meta));
        
        long start = System.currentTimeMillis();
        
        for (int i = 0; i < 100; i++) {
            segment.append(ByteBuffer.wrap(bigBuf));
        }
        segment.force();
        
        long end = System.currentTimeMillis();
        
        // should be able to do it in 1 second.
        Assert.assertTrue(end-start < 1000);
        TestUtil.removeDir(dir);
    }    
    
    @Test
    public void testMarkCanBeRead() throws IOException {
        byte[] buf = new byte[1024];
        random.nextBytes(buf);
        
        File dir= TestUtil.getRandomTempDir();
        System.out.println(dir.getAbsolutePath());
        File data = new File(dir, "segment.log");
        File meta = new File(dir, "segment.meta");
        
        Segment segment = Segment.forWrite(new FileDataIO(data, 0), new FileMetaIO(meta));
        Assert.assertEquals(0, segment.getMark());
        Assert.assertEquals(0, segment.getFilePointer());
        
        segment.append(ByteBuffer.wrap(buf));
        segment.append(ByteBuffer.wrap(buf));
        long fp = segment.getFilePointer();
        Assert.assertEquals((buf.length + 4) * 2, fp);
        Assert.assertEquals(0, segment.getMark());
        
        segment.mark(fp);
        Assert.assertEquals(fp, segment.getFilePointer());
        Assert.assertEquals(fp, segment.getMark());
        
        segment.close();
        segment = null; // so I know it will not be accessed.
        
        Segment readSegment = Segment.forRead(new FileDataIO(data, 0), new FileMetaIO(meta));
        
        Assert.assertEquals(fp, readSegment.getMark());
        Assert.assertEquals(0, readSegment.getFilePointer());
        
        TestUtil.removeDir(dir);
    }
    
    @Test
    public void testReadFromMark() throws IOException {
        File dir = TestUtil.getRandomTempDir();
        System.out.println(dir);
        File data = new File(dir, "segment.log");
        File meta = new File(dir, "segment.meta");
        
        Segment segment = Segment.forWrite(new FileDataIO(data, 0), new FileMetaIO(meta));
        for (int i = 0; i < 100; i++) {
            byte[] buf = new byte[i+1];
            random.nextBytes(buf);
            segment.append(ByteBuffer.wrap(buf));
        }
        
        Assert.assertEquals(5050 + 100*4, segment.getFilePointer());
        
        segment.mark(15 + (4*5)); // skips the first 5 entries.
        segment.force();
        segment.close();
        segment = null;
 
        Segment readSegment = Segment.forRead(new FileDataIO(data, 0), new FileMetaIO(meta));
        int counter = 6;
        for (ByteBuffer buf : readSegment.readFromMark()) {
            Assert.assertEquals(counter, buf.remaining());
            counter += 1;
            if (counter > 500) {
                throw new IOException("Exceeded expected buffer count");
            }
        }
        
        Assert.assertEquals(101, counter);
        
        TestUtil.removeDir(dir);
    }
    
    @Test(expected = IOException.class)
    public void testReadSegmentCannotBeAppendedTo() throws IOException {
        File dir = TestUtil.getRandomTempDir();
        Assert.assertTrue(dir.list().length == 0);
        System.out.println(dir);
        File data = new File(dir, "segment.log");
        File meta = new File(dir, "segment.meta");
        
        Segment segment = Segment.forWrite(new FileDataIO(data, 0), new FileMetaIO(meta));
        for (int i = 0; i < 100; i++) {
            byte[] buf = new byte[i+1];
            random.nextBytes(buf);
            segment.append(ByteBuffer.wrap(buf));
        }
        segment.close();
        
        Assert.assertTrue(dir.list().length > 0);
        
        segment = Segment.forRead(new FileDataIO(data, 0), new FileMetaIO(meta));
        segment.append(ByteBuffer.wrap(new byte[100]));
        
        // todo: how to delete this dir. need to create @Before and @After fixtures.
        
    }
    
    @Test
    public void testSegmentReading() throws IOException {
        File dir = TestUtil.getRandomTempDir();
        Assert.assertTrue(dir.list().length == 0);
        System.out.println(dir);
        File data = new File(dir, "segment.log");
        File meta = new File(dir, "segment.meta");
        
        Segment segment = Segment.forWrite(new FileDataIO(data, 0), new FileMetaIO(meta));
        for (int i = 0; i < 100; i++) {
            byte[] buf = new byte[i+1];
            random.nextBytes(buf);
            segment.append(ByteBuffer.wrap(buf));
        }
        // each entry takes one byte more than the next.
        // 0:5, 1:6, 2:7, 3:8. so the next entry begins at offset 5+6+7+8=26 and should have us expecting 5 bytes.
        segment.mark(26);
        Assert.assertEquals(26, segment.getMark());
        segment.close();
        
        Assert.assertTrue(dir.list().length > 0);
        
        segment = Segment.forRead(new FileDataIO(data, 0), new FileMetaIO(meta));
        
        Assert.assertEquals(26, segment.getMark());
        int bytesInNextEntry = segment.readInt(segment.getMark());
        
        Assert.assertEquals(5, bytesInNextEntry);
        
        TestUtil.removeDir(dir);
    }
}
