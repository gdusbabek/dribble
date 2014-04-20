# Dribble

A journaling library (commit log) written in java. Supports marking + recovery.

### TODOs

 - `Journal.recover()`. `Segment.readFromMark()` is implemented, but I haven't put the recovery logic in `Journal` to go to the right segment and start reading all data from there.
 - Robust tests. They are pretty weak right now.
 - A compaction API. right now, files are deleted by the write thread. This work should be done by another threadpool.


### Examples

Later.
