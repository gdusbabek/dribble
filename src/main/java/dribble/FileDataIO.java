package dribble;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileDataIO implements DataIO {
    
    private final File file;
    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final int generation;
    
    public FileDataIO(File f, int generation) {
        this.file = f;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException ex) {
            throw new IOError(ex);
        }
        this.generation = generation;
    }
    public void close() throws IOException {
        fc.close();
        raf.close();
    }

    public void sync() throws IOException {
        fc.force(true);
    }

    public long position() throws IOException {
        return fc.position();
    }

    public void seek(long l) throws IOException {
        fc.position(l);
    }

    public int write(ByteBuffer buf) throws IOException {
        return fc.write(buf);
    }

    public int read(ByteBuffer buf) throws IOException {
        return fc.read(buf);
    }

    public int generation() {
        return generation;
    }
    
    public void delete() throws IOException {
        if (!file.delete())
            throw new IOException("Could not delete " + file.getAbsolutePath());
    }
}
