package dribble;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileMetaIO implements MetaIO {
    private final RandomAccessFile raf;
    private final File deletable;
    
    public FileMetaIO(File f) {
        try {
            raf = new RandomAccessFile(f, "rw");
            deletable = f;
        } catch (FileNotFoundException ex) {
            throw new IOError(ex);
        }
    }
    public void close() throws IOException {
        raf.close();
    }

    public void sync() throws IOException {
        raf.getFD().sync();
    }

    public void seek(long l) throws IOException {
        raf.seek(l);
    }

    public void writeLong(long l) throws IOException {
        raf.writeLong(l);
    }

    public long readLong() throws IOException {
        return raf.readLong();
    }

    public void delete() throws IOException {
        if (!deletable.delete())
            throw new IOException("Could not delete " + deletable.getAbsolutePath());
    }
}
