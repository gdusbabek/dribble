package dribble;

import java.io.IOException;

public interface SegmentFactory {
    public Segment next() throws IOException;
    public Iterable<Segment> getSegments() throws IOException;
}
