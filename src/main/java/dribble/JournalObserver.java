package dribble;

import java.nio.ByteBuffer;

public interface JournalObserver {
    public void recover(ByteBuffer buf);
}
