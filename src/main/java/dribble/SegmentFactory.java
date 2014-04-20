package dribble;

import java.io.IOException;

public interface SegmentFactory {
    public Segment next() throws IOException;
}
