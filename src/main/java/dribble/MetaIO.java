package dribble;

import java.io.IOException;

public interface MetaIO {
    public void close() throws IOException;
    public void sync() throws IOException;
    public void seek(long l) throws IOException;
    public void writeLong(long l) throws IOException;
    public long readLong() throws IOException;
    public void delete() throws IOException;
}
