package dribble;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private Receipt newestReceipt = null;
    
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
    }
    
    // multiple threads will be calling this.
    public Receipt append(ByteBuffer buf) throws IOException {
        // todo: block on high water (future feature).
        Future<WriteDetails> details = writePool.submit(new Commit(buf));
        Receipt receipt = new Receipt(details);
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
        if (newestReceipt == null || receipt.compareTo(newestReceipt) < 0) {
            return; 
        }
        
        maybeSwitchReceipts(receipt);
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
    
    // CONCURRENT_ALERT: locking happens here.
    private void maybeSwitchReceipts(Receipt receipt) throws IOException {
        receiptLock.lock();
        try {
            // check again to prevent a race.
            if (newestReceipt == null || receipt.compareTo(newestReceipt) < 0) {
                return; 
            }
            
            newestReceipt = receipt;
            newestReceipt.details.seg.mark(newestReceipt.details.pos);
            
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
        
        public Receipt(Future<WriteDetails> futureDetails) {
            
            this.futureDetails = futureDetails;
        }
        
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
            if (!isDurable() || !o.isDurable()) {
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
