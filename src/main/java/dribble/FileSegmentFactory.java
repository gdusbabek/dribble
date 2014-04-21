package dribble;

import com.google.common.collect.Lists;
import com.sun.swing.internal.plaf.metal.resources.metal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSegmentFactory implements SegmentFactory {
    private static final FileFilter segmentFilter = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.getName().endsWith("-segment.log");
        }
    };
    private static final Comparator<File> segmentComparator = new Comparator<File>() {
        public int compare(File a, File b) {
            int ia = Integer.parseInt(a.getName().split("\\-", -1)[0]);
            int ib = Integer.parseInt(b.getName().split("\\-", -1)[0]);
            return ia - ib;
        }
    };
    
    private final File dir;
    private final long bytesBetweenSync;
    private final long millisBetweenSync;
    private final long writesBetweenSync;
    private final boolean syncAlways;
    
    private final AtomicInteger fileId = new AtomicInteger(0);
    
    private FileSegmentFactory(File dir, long bytesBetweenSync, long millisBetweenSync, long writesBetweenSync, boolean syncAlways) {
        this.dir = dir;
        this.bytesBetweenSync = bytesBetweenSync;
        this.millisBetweenSync = millisBetweenSync;
        this.writesBetweenSync = writesBetweenSync;
        this.syncAlways = syncAlways;
        
        int maxId = -1;
        for (File f : dir.listFiles(segmentFilter)) {
            int id = Integer.parseInt(f.getName().split("\\-", -1)[0]);
            maxId = Math.max(maxId, id);
        }
        maxId += 1;
        fileId.set(maxId + 1);
    }
    
    public Segment next() throws IOException {
        int id = fileId.getAndIncrement();
        File dataFile = new File(dir, String.format("%d-segment.log", id));
        File metaFile = new File(dir, String.format("%d-meta.log", id));
        DataIO dataIO = new FileDataIO(dataFile, id);
        MetaIO metaIO = new FileMetaIO(metaFile);
        Segment segment = Segment.forWrite(dataIO, metaIO);
        segment.syncAfterBytes(bytesBetweenSync);
        segment.syncAfterTime(millisBetweenSync);
        segment.syncAfterWrites(writesBetweenSync);
        segment.syncAlways(syncAlways);
        return segment;
    }

    public Iterable<Segment> getSegments() throws IOException {
        List<File> files = Lists.newArrayList(dir.listFiles(segmentFilter));
        Collections.sort(files, segmentComparator);
        List<Segment> segments = new ArrayList<Segment>();
        for (File f : files) {
            int id = Integer.parseInt(f.getName().split("\\-", -1)[0]);
            DataIO dataIO = new FileDataIO(f, id);
            MetaIO metaIO = new FileMetaIO(new File(dir, String.format("%d-meta.log", id)));
            Segment segment = Segment.forRead(dataIO, metaIO);
            segments.add(segment);
        }
        return segments;
    }

    public static class FileSegmentFactoryBuilder {
        private File dir;
        private long bytesBetweenSync = 0x0000000008000000;
        private long millisBetweenSync = 10000;
        private long writesBetweenSync = 10000;
        private boolean syncAlways = false;

        private FileSegmentFactoryBuilder() { }
        
        public FileSegmentFactoryBuilder withDirectory(File dir) {
            this.dir = dir;
            return this;
        }
        
        public static FileSegmentFactoryBuilder newBuilder() {
            return new FileSegmentFactoryBuilder();
        }
        
        public FileSegmentFactoryBuilder withBytesBetweenSync(long l) {
            this.bytesBetweenSync = l;
            return this;
        }
        
        public FileSegmentFactoryBuilder withMillisBetweenSync(long l) {
            this.millisBetweenSync = l;
            return this;
        }
        
        public FileSegmentFactoryBuilder withWritesBetweenSync(long l) {
            this.writesBetweenSync = l;
            return this;
        }
        
        public FileSegmentFactoryBuilder withSyncAfterEveryAppend(boolean b) {
            this.syncAlways = b;
            return this;
        }
        
        public SegmentFactory build() {
            return new FileSegmentFactory(dir, bytesBetweenSync, millisBetweenSync, writesBetweenSync, syncAlways);
        }
    }
}
