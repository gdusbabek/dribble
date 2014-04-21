package dribble;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Util {
    
    public static int readInt(ByteBuffer buf) throws IOException {
        if (buf.remaining() < 4) {
            throw new IOException("Not enough bytes in buffer to read integer");
        }
        int ch1 = 0x0000ffff & buf.get();
        int ch2 = 0x0000ffff & buf.get();
        int ch3 = 0x0000ffff & buf.get();
        int ch4 = 0x0000ffff & buf.get();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }
}
