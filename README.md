# Dribble

A journaling library (commit log) written in java. Supports marking + recovery.

### TODOs

 - Consider adding a sync command to the `Receipt` API.
 - Robust tests. They are pretty weak right now.
 - A compaction API. right now, files are deleted by the write thread. This work should be done by another threadpool.
 - All that maven crap.


### Example

Create a segment factory. Segments are where the writing actually happens. 
Segments know when to sync themselves, based on settings supplied by the factory.

    File dir = new File("/tmp/my_journal");
    SegmentFactory segmentFactory = FileSegmentFactory.FileSegmentFactoryBuilder
                    .newBuilder()
                    .withBytesBetweenSync(0x00080000)
                    .withMillisBetweenSync(1000)
                    .withSyncAfterEveryAppend(false)
                    .withWritesBetweenSync(100)
                    .withDirectory(dir)
                    .build();
            
    Journal journal = new Journal(segmentFactory, 0x00100000);
    
Every write is asynchronous. The call produces a `Receipt` that can then be used to demand that a write (and every write
before it) be made durable. This is not the same as `mark()`ing a journal.
    
    // completely asynchronous.
    Receipt receipt0 = journal.append(ByteBuffer.wrap(getRandomBytes()));
    Receipt receipt1 = journal.append(ByteBuffer.wrap(getRandomBytes()));
    Receipt receipt2 = journal.append(ByteBuffer.wrap(getRandomBytes()));
    
    // wait until a receipt is durable (not the same as a file system sync).
    receipt0.await(5000);
    
Mark the journal. This indicates that all writes prior (and including the mark) are no long needed or cared
about. If there are older writes in other segments, those segments may be deleted.

This invokes a blocking process where the receipt is verified durable, then metadata associated with the segment
the receipt belongs to is updated to indicate its mark, which can be used for recovery later. 

If a newer receipt has already been marked in this journal, this operation becomes a no-op.
    
    journal.mark(receipt1);
    
    // shut down the journal.
    journal.drain();
    journal.close();
    
Recreate the journal so that we can recover writes (just one in this case). Old segments are opened in read-only mode.

    journal = new Journal(segmentFactory, 0x00100000);
    
    // recover lost entries. in this case, there is only one.
    JournalObserver observer = new JournalObserver() {
        public void recover(ByteBuffer buf) {
            // should get called once!
        }
    };
    journal.recover(observer);
    
    // since recovery does not update any data or metadata, you can call it again.
    journal.recover(observer);
    
This new journal can still be appended to. However, once you `mark()` it again, any old segments, including those
that were recovered, are available to be collected for deletion.

### License

Apache 2.0. Go for it.
    