package dribble;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface DataIO {
    public void close() throws IOException;
    public void sync() throws IOException;
    public long position() throws IOException;
    public void seek(long l) throws IOException;
    public int write(ByteBuffer buf) throws IOException;
    public int read(ByteBuffer buf) throws IOException;
    public int generation();
    public void delete() throws IOException;
    
}
