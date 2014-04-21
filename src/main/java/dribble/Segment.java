package dribble;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/** Not designed to be thread safe. Only use by a single executor. */
public class Segment implements Comparable<Segment> {
    private static final long FORGET = -1;
    private ByteBuffer LENGTH_BUF = ByteBuffer.wrap(new byte[4]);
    
    private final DataIO dataIO;
    private final MetaIO meta;
    
    private volatile long bytesSinceLastSync = 0;
    private volatile long writesSinceSync = 0;
    private volatile long lastSync = System.currentTimeMillis();
    
    private long mark = 0;
    private long filePointer = 0;
    
    // reasons to sync.
    private long syncAfterBytes = 0x0000000008000000; // 128mb
    private long syncAfterTime = 10000; // 10s
    private long syncAfterWrites = 10000;
    private boolean alwaysSync = false;
    
    public static Segment forWrite(DataIO output, MetaIO meta) throws IOException {
        Segment seg = new Segment(output, meta);
        seg.mark(0);
        return seg;
    }
    
    public static Segment forRead(DataIO output, MetaIO meta) throws IOException {
        // todo: needs its own class.
        Segment seg = new Segment(output, meta) {
            @Override
            public void mark(long pos) throws IOException {
                throw new IOException("Read only mode!");
            }

            @Override
            public void append(ByteBuffer buf) throws IOException {
                throw new IOException("Read only mode!");
            }
        };
        seg.loadMark();
        return seg;
    }
    
    private Segment(DataIO dataIO, MetaIO meta) {
        this.dataIO = dataIO;
        this.meta = meta;
    }
    
    public int compareTo(Segment o) {
        return generation() - o.generation();
    }
    
    public void syncAfterBytes(long thisMany) { syncAfterBytes = thisMany; }
    public void syncAfterWrites(long thisMany) { syncAfterWrites = thisMany; }
    public void syncAfterTime(long millis) { syncAfterTime = millis; }
    public void syncAlways(boolean b) { alwaysSync = b; }
    public int generation() { return dataIO.generation(); }
    
    public long getFilePointer() { return filePointer; }
    public long getMark() { return mark; }
    
    public void close() throws IOException {
        force();
        dataIO.close();
        meta.close();
    }
    
    public void force() throws IOException {
        dataIO.sync();
        meta.sync();
    }
    
    public void mark(long pos) throws IOException {
        meta.seek(0);
        meta.writeLong(pos);
        meta.sync();
        this.mark = pos;
    }
    
    public void softMark(long pos) {
        this.mark = pos;
    }
    
    public void forget() throws IOException {
        mark(FORGET);
    }
    
    public void delete() throws IOException {
        close();
        dataIO.delete();
        meta.delete();
    }
    
    public Segment position(long pos) {
        this.filePointer = pos;
        return this;
    }
    
    public int readInt(long position) throws IOException {
        dataIO.seek(position);
        this.filePointer = position;
        ByteBuffer buf = ByteBuffer.wrap(new byte[4]);
        dataIO.read(buf);
        buf.flip();
        return Util.readInt(buf);
    }
    
    public void append(ByteBuffer buf) throws IOException {
        long now = System.currentTimeMillis();
        long remaining = buf.remaining();
        
        LENGTH_BUF.clear();
        LENGTH_BUF.putInt((int)remaining);
        LENGTH_BUF.flip();
        writeFully(LENGTH_BUF, 4);
        writeFully(buf, buf.remaining());
        filePointer = dataIO.position();
        
        bytesSinceLastSync += remaining;
        writesSinceSync += 1;
        
        maybeSync(now);
    }
    
    private void writeFully(ByteBuffer buf, int remaining) throws IOException {
        int wrote = 0;
        while (wrote < remaining) {
            wrote += dataIO.write(buf);
        }
    }
    
    public Iterable<ByteBuffer> readFromMark() throws IOException {
        return readFromMark(mark);
    }
    
    public Iterable<ByteBuffer> readFromMark(long softMark) throws IOException {
        dataIO.seek(softMark);
        return new Iterable<ByteBuffer>() {
            public Iterator<ByteBuffer> iterator() {
                return new Iterator<ByteBuffer>() {
                    private ByteBuffer next = preReadNextOrNull();
                    
                    public boolean hasNext() {
                        return next != null;
                    }

                    public ByteBuffer next() {
                        ByteBuffer returnThis = next;
                        next = preReadNextOrNull();
                        return returnThis;
                    }

                    public void remove() {
                        throw new RuntimeException("Illegal call");
                    }
                    
                    private ByteBuffer preReadNextOrNull() {
                        int numBytes = readLength();
                        if (numBytes < 1) {
                            return null;
                        }
                        ByteBuffer buf = ByteBuffer.allocate(numBytes);
                        try {
                            readFully(buf);
                        } catch (IOException ex) {
                            return null;
                        }
                        buf.flip();
                        return buf;
                    }
                    
                    private int readLength() {
                        LENGTH_BUF.clear();
                        assert LENGTH_BUF.remaining() == 4;
                        try {
                            readFully(LENGTH_BUF);
                        } catch (IOException ex) {
                            return 0;
                        }
                        LENGTH_BUF.flip();
                        int numBytes = LENGTH_BUF.getInt();
                        return numBytes;
                    }
                    
                    private void readFully(ByteBuffer buf) throws IOException {
                        int length = buf.remaining();
                        int read = 0;
                        while (read < length && buf.remaining() > 0) {
                            int justRead =  dataIO.read(buf);
                            if (justRead < 0) {
                                throw new IOException("EOF");
                            }
                        }
                    }
                };
            }
        };
    }
    
    private void maybeSync(long now) throws IOException {
        boolean sync = false;
        
        // sync if we wrote enough bytes.
        if (syncAfterBytes > 0 && bytesSinceLastSync > syncAfterBytes) {
            sync = true;
        } 
        
        // sync if it's been a while.
        else if (syncAfterTime > 0 && now - lastSync > syncAfterTime) {
            sync = true;
        }
        
        // sync if we've written a lot of times.
        else if (syncAfterWrites > 0 && writesSinceSync > syncAfterWrites) {
            sync = true;
        } 
        
        // sync if we always should.
        else if (alwaysSync) {
            sync = true;
        }
        
        if (sync) {
            dataIO.sync();
            bytesSinceLastSync = 0;
            lastSync = now;
            writesSinceSync = 0;
        }
    }
    
    private void loadMark() throws IOException {
        meta.seek(0);
        this.mark = meta.readLong();
    }
    
    
}
