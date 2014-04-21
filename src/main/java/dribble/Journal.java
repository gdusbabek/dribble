package dribble;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Journal {
    private static final Logger log = LoggerFactory.getLogger(Journal.class);
    
    private final SegmentFactory segmentFactory;
    private final long segmentSize;
    private final Lock segmentLock = new ReentrantLock(true);
    private final Lock receiptLock = new ReentrantLock(true);
    
    private final ThreadPoolExecutor writePool;
    private final BlockingQueue<Runnable> writeQueue = new LinkedBlockingQueue<Runnable>();
    private final LinkedList<Segment> oldSegments = new LinkedList<Segment>();
    
    private Segment current = null;
    private volatile Receipt newestMarkReceipt = null;
    private volatile Receipt newestReceipt = null; // not used. the idea is that in the future we can mark(latest) without having a reference to the actual receipt.
    
    private volatile int appendsSinceOpen = 0;
    
    public Journal(SegmentFactory segmentFactory, long segmentSize) {
        this.segmentFactory = segmentFactory;
        this.segmentSize = segmentSize;
        
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
                    public void uncaughtException(Thread t, Throwable e) {
            log.error("Thread dieing due to exception {}", t.getName());
            log.error(e.getMessage(), e);
            }
        };
        final ThreadFactory commitThreadFactory = new ThreadFactoryBuilder().setDaemon(false).setNameFormat("entrust-commit-log").setPriority(Thread.NORM_PRIORITY).setUncaughtExceptionHandler(uncaughtExceptionHandler).build();
        final RejectedExecutionHandler commitRejectedExecutionHandler = new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // could not accept task because of resource exhaustion. we need to let the caller know about this.
            throw new RejectedExecutionException("Cannot commit this item at this time");
            }
        };
        
        this.writePool = new ThreadPoolExecutor(
            1, 1,
            30, TimeUnit.SECONDS, 
            writeQueue,
            commitThreadFactory, 
            commitRejectedExecutionHandler);
        
        // position segment
        // set receipt from last mark.
        Receipt latest = null;
        try {
            for (Segment oldSegment : segmentFactory.getSegments()) {
                if (latest == null) {
                    latest = receiptFor(oldSegment);
                } else if (oldSegment.getMark() > 0) {
                    latest = receiptFor(oldSegment);
                }
                oldSegments.add(oldSegment);
            }
            newestReceipt = latest;
        } catch (IOException ex) {
            throw new IOError(ex);
        }
    }
    
    private static Receipt receiptFor(Segment segment) throws IOException {
        WriteDetails details = new WriteDetails(segment, segment.getMark());
        return new Receipt(details, segment.readInt(segment.getMark()));
    }
    
    // multiple threads will be calling this.
    public Receipt append(ByteBuffer buf) throws IOException {
        // todo: block on high water (future feature).
        int length = buf.remaining();
        Future<WriteDetails> details = writePool.submit(new Commit(buf));
        Receipt receipt = new Receipt(details, length);
        newestReceipt = receipt;
        appendsSinceOpen += 1; // todo: not threadsafe.
        return receipt;
    }
    
    // multiple threads can call this and pass in all kinds of garbage.
    public void mark(Receipt receipt) throws IOException {
        // first, wait for it to be durable.
        try {
            receipt.await(Long.MAX_VALUE);
        } catch (TimeoutException ex) {
            throw new IOException("Waited too long");
        }
        
        // if we have alraedy been marked at a later place, return.
        if (receipt.compareTo(newestMarkReceipt) < 0) {
            return; 
        }
        
        maybeSwitchReceipts(receipt);
    }
    
    public int getBacklogSize() {
        return this.writeQueue.size();
    }
    
    public int getAppendsSinceOpen() {
        return this.appendsSinceOpen;
    }
    
    public void drain() throws Exception {
        writePool.shutdown();
        boolean safe = false;
        Exception willThrow = null;
        try {
            safe = writePool.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            willThrow = ex;
        }
        
        if (!safe) {
            writePool.shutdownNow();
            log.warn("Unsafe commit log drain");
        }
        
        if (willThrow != null)
            throw willThrow;
    }
    
    public void close() throws IOException {
        // stop writing with prejudice.
        writePool.shutdownNow();
        while (oldSegments.size() > 0) {
            try {
                oldSegments.removeFirst().close();
            } catch (IOException ex) {
                log.warn(ex.getMessage(), ex);
            }
        }
        if (current != null) {
            current.close();
        }
    }
    
    // it is expected that recovery happens during a period where there are no appends happening. The API doesn't
    // prevent you from abusing that expectation, but you are likely to create a race (e.g. recovering FOREVER) if you
    // recover while appends are happening.
    public void recover(JournalObserver observer) throws IOException {
        // copy the receipt and clone the segments.
        // todo: handle null receipt.
        
        LinkedList<Segment> segments = new LinkedList<Segment>(this.oldSegments);
        if (current != null) {
            segments.add(current);
        }
        
        // ignore all the segments before the mark.
        Receipt mark = newestMarkReceipt;
        while (mark != null && segments.peek() != mark.details.seg) {
            segments.removeFirst();
        }
        
        // attempt a short circuit to avoid hairy logic to follow.
        if (segments.size() == 0) {
            return;
        }
        
        // the mark represents the last entry that was durable. so we do not want to recover it. this flag helps us
        // skip that entry.
        boolean firstSkipped = false;
        
        // recover the first segment, which is likely to be a partial (use the mark it specifies).
        Segment partial = segments.removeFirst();
        for (ByteBuffer buf : partial.readFromMark()) {
            if (firstSkipped) {
                observer.recover(buf);
            }
            firstSkipped = true;
        }
        
        // recover the rest of the segments forcing a soft mark=0.
        while (segments.size() > 0) {
            Segment s = segments.removeFirst();
            for (ByteBuffer buf : s.readFromMark(0)) {
                if (firstSkipped) {
                    observer.recover(buf);
                }
                firstSkipped = true;
            }
        }
        
        // that's it. we are not going to modify the state of any of the segments. let the user call mark on
        // something to force that. 
    }
    
    // CONCURRENT_ALERT: locking happens here.
    private void maybeSwitchReceipts(Receipt receipt) throws IOException {
        receiptLock.lock();
        try {
            // check again to prevent a race.
            if (receipt.compareTo(newestMarkReceipt) < 0) {
                return; 
            }
            
            Receipt oldMarkReceipt = newestMarkReceipt;
            newestMarkReceipt = receipt;
            // set the new mark.
            newestMarkReceipt.details.seg.mark(newestMarkReceipt.details.pos);
            // clear the old mark.
            if (oldMarkReceipt != null) {
                oldMarkReceipt.details.seg.mark(0);
            }
            
            while (oldSegments.size() > 0 && oldSegments.peek() != receipt.details.seg) {
                try {
                    Segment old = oldSegments.removeFirst();
                    old.forget();
                    old.delete();
                } catch (IOException ex) {
                    log.warn(ex.getMessage(), ex);
                }
            }
            // otherwise.
        } finally {
            receiptLock.unlock();
        }
    }
    
    // gets called in a single thread context.
    // todo: is syncing even necessary?
    // CONCURRENT_ALERT: locking happens here.
    private void maybeSwitchSegments() throws IOException {
        if (current == null)
            current = segmentFactory.next();
        
        if (current.getFilePointer() > segmentSize) {
            segmentLock.lock();
            try {
                current.force();
                oldSegments.add(current); 
                current = segmentFactory.next();
            } finally {
                segmentLock.unlock();
            }
        }
    }
    
    private class Commit implements Callable<WriteDetails> {
        private final ByteBuffer buf;
        public Commit(ByteBuffer buf) {
            this.buf = buf;
        }

        public WriteDetails call() throws Exception {
            maybeSwitchSegments();
            Segment seg = current;
            long pos = seg.getFilePointer();
            seg.append(buf);
            return new WriteDetails(seg, pos);
        }
    }
    
    public static class Receipt implements Comparable<Receipt> {
        private final Future<WriteDetails> futureDetails;
        private WriteDetails details;
        private final int length;
        
        public Receipt(WriteDetails details, int length) {
            this.details = details;
            this.length = length;
            this.futureDetails = new NoOpFuture<WriteDetails>(details);
        }
        
        public Receipt(Future<WriteDetails> futureDetails, int length) {            
            this.futureDetails = futureDetails;
            this.length = length;
        }
        
        public int length() { return length; }
        
        public boolean isDurable() {
            return futureDetails.isDone();
        }
        
        public void await(long millis) throws TimeoutException, IOException {
            try {
                details = this.futureDetails.get(millis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (ExecutionException ex) {
                Throwable realProblem = ex.getCause();
                if (realProblem instanceof IOException) {
                    throw (IOException)realProblem;
                } else {
                    throw new IOException(realProblem);
                }
            }
        }

        // precondition. both receipts are durable.
        public int compareTo(Receipt o) {
            if (!isDurable())
                throw new RuntimeException("Must be durable to compare");
            
            // this comes after null.
            if (o == null)
                return 1;
            
            if (!o.isDurable()) {
                throw new RuntimeException("Cannot compare non-durable receipts");
            }
            
            return details.seg.compareTo(o.details.seg);
        }
    }
    
    public static class WriteDetails {
        private final Segment seg;
        private final long pos;
        
        public WriteDetails(Segment seg, long pos) {
            this.seg = seg;
            this.pos = pos;
        }
    }
}
