package org.apache.hadoop.ozone.om.ratis;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.tuple.Pair;
import net.jcip.annotations.ThreadSafe;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("checkstyle:MissingJavadocType")
@ThreadSafe // TODO check
public final class OzoneManagerBatchWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerBatchWriter.class);
  /** Represents the count of entries added to the journal queue. */
  private static final String MASTER_JOURNAL_FLUSH_BATCH_TIME_US =
      "master.journal.flush.batch.time.us";
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private final AtomicLong mCounter;
  /** Represents the count of entries flushed to the journal writer. */
  private final AtomicLong mFlushCounter;

  private final Set<FlushTicket> mTicketSet = new ConcurrentHashSet<>();

  private final long mFlushBatchTimeNs;

  private final OzoneManagerRatisServer ratisServer;

  private ExecutorService executorService;  // Thread pool for handling futures
  private ExecutorService doflushsExecutor;  // Thread pool for handling futures
  private ExecutorService doFuturesExecutor;  // Thread pool for handling futures

  // TODO what is mJournalSinks??
  /**
   * Dedicated thread for writing and flushing entries in journal queue.
   * It goes over the {@code mTicketList} after every flush session and releases waiters.
   */
  private ArrayList<ConcurrentLinkedQueue<Pair<OMRequest, CompletableFuture<OMResponse>>>> queues;
  /**
   * Control flag that is used to instruct flush thread to exit.
   */
  private volatile boolean mStopFlushing = false;
  private Call call;

  private int doflushsExecutorCnt;

  public OzoneManagerBatchWriter(OzoneManagerRatisServer ratisServer) {
    this.executorService = Executors.newFixedThreadPool(10);
    this.ratisServer = ratisServer;
    doflushsExecutorCnt = 5;
    queues = new ArrayList<>();
    this.doflushsExecutor = Executors.newFixedThreadPool(doflushsExecutorCnt);
    this.doFuturesExecutor = Executors.newFixedThreadPool(doflushsExecutorCnt);
    call = Server.getCurCall().get();
    for (int i = 0; i < doflushsExecutorCnt; i++) {
      RaftJournalWriter journalWriter = new RaftJournalWriter(ratisServer);
      final ConcurrentLinkedQueue<Pair<OMRequest, CompletableFuture<OMResponse>>> queue = new ConcurrentLinkedQueue<>();
      queues.add(queue);
      doflushsExecutor.submit(() -> doFlush(queue, journalWriter), "BatchWriterThread-%d");
      doFuturesExecutor.submit(() -> doFutures(journalWriter), "FuturesThread-%d");
    }
    mCounter = new AtomicLong(0);
    mFlushCounter = new AtomicLong(0);
    mFlushBatchTimeNs =
        Long.parseLong(conf.get(MASTER_JOURNAL_FLUSH_BATCH_TIME_US, "100000")) *
            1000;
  }

  /**
   * Appends a {@link OMRequest} for writing to the journal.
   *
   * @param entry the {@link OMRequest} to append
   * @return a counter for the entry, for flushing
   */
  public CompletableFuture<OMResponse> appendEntry(OMRequest entry) {
//    LOG.info("Append entry {}", entry.getCmdType());
//    LOG.info("appendEntry getClientId {}", ProtobufRpcEngine.Server.getClientId());
//    LOG.info("appendEntry getCallId {}", ProtobufRpcEngine.Server.getCallId());
    CompletableFuture<OMResponse> future = new CompletableFuture<>();
    queues.get((int)(mCounter.incrementAndGet() % doflushsExecutorCnt)).add(Pair.of(entry, future));
    mFlushSemaphore.release();
    return future;
  }

  private void doFutures(RaftJournalWriter journalWriter) {
    LOG.info("doFutures start {}", Thread.currentThread());
    while (!Thread.currentThread().isInterrupted()) {
      try {
        final CompletableFuture<RaftClientReply> journalWriterFuture = journalWriter.takeFuture();
        executorService.submit(() -> processFuture(journalWriterFuture, journalWriter));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void processFuture(CompletableFuture<RaftClientReply> future, RaftJournalWriter journalWriter) {
    try {
      RaftClientReply reply = future.get();
      OMResponse omResponse = ratisServer.createOmResponse(
          OMRequest.newBuilder().setClientId("Client-123411115")
              .setCmdType(Type.UnknownCommand).build(), reply);
      for (OMResponse response : omResponse.getResponsesList()) {
        CompletableFuture<OMResponse> responseFuture = journalWriter.getFuture(response.getSequenceNumber());
        responseFuture.complete(response);
      }
    } catch (Exception e) {
      LOG.error("Error processing future", e);
    }
  }

  private void doFlush(ConcurrentLinkedQueue<Pair<OMRequest, CompletableFuture<OMResponse>>> queue,
      RaftJournalWriter journalWriter) {
    Server.getCurCall().set(call);
    LOG.info("doFlush start {}", Thread.currentThread());
    while (!mStopFlushing) {
      while (queue.isEmpty() &&
          !mStopFlushing) { // TODO, return immediately when the mQueue be inserted a request
        try {
          // Wait for permit up to batch timeout.
          // PS: We don't wait for permit indefinitely in order to process
          // queued entries proactively.
          if (mFlushSemaphore.tryAcquire(mFlushBatchTimeNs,
              TimeUnit.NANOSECONDS)) {
//            LOG.info("doFlush acquired a permit ");
            break;
          }
        } catch (InterruptedException ie) {
          break;
        }
      }

      try {
        long startTime = System.nanoTime();
        // Write pending entries to journal.
//        while (!queue.isEmpty()) {
        while (true) {
          // Get, but do not remove, the head entry.
          Pair<OMRequest, CompletableFuture<OMResponse>> entry =
              queue.peek(); // TODO requestAllowed
          if (entry == null) {
            // No more entries in the queue. Break write session.
//            LOG.info("entry == null");
            continue;
          }
          journalWriter.write(entry);
          // Remove the head entry, after the entry was successfully written.
          queue.poll();

//          if (((System.nanoTime() - startTime) >= mFlushBatchTimeNs) &&
//              !mStopFlushing) {
//            // This thread has been writing to the journal for enough time. Break out of the
//            // infinite while-loop.
//            LOG.info("doFlush enqueue timeout break");
//            break;
//          }
        }
//        LOG.info("doflush enfore");
//        journalWriter.flush();
      } catch (IOException e) {

      }
    }
  }

  /**
   * Closes the async writer.
   * PS: It's not guaranteed for pending entries to be flushed.
   *     Use ::flush() for guaranteeing the entries have been flushed.
   */
  public void close() {
    stop();
  }


  /**
   * Used to give permits to flush thread to start processing immediately.
   */
  private final Semaphore mFlushSemaphore = new Semaphore(0, true);

  void stop() {
    // Set termination flag.
    mStopFlushing = true;
    // Give a permit for flush thread to run, in case it was blocked on permit.
    mFlushSemaphore.release();

//    try {
//
//    } catch (InterruptedException ie) {
//      Thread.currentThread().interrupt();
//    } finally {
//
//      // Try to reacquire the permit.
//      mFlushSemaphore.tryAcquire();
//    }
  }

  /**
   * .
   */
  public static class RaftJournalWriter {

    private static final Logger LOG =
        LoggerFactory.getLogger(RaftJournalWriter.class);

    private volatile boolean mClosed = false;

    public OzoneManagerRatisServer getRatisServer() {
      return ratisServer;
    }

    private final OzoneManagerRatisServer ratisServer;

    private OMRequest.Builder oMRequestsBuilder;

    public CompletableFuture<OMResponse> getFuture(long index) {
      return responseFutures.remove(index);
    }

    private final ConcurrentHashMap<Long, CompletableFuture<OMResponse>>
        responseFutures = new ConcurrentHashMap<>();

    public CompletableFuture<RaftClientReply> takeFuture()
        throws InterruptedException {
      return futures.take();
    }

    private final LinkedBlockingQueue<CompletableFuture<RaftClientReply>> futures = new LinkedBlockingQueue<>();
    private final AtomicLong mNextSequenceNumberToWrite = new AtomicLong(0);
    // Should be presisted?
    private final AtomicLong mLastSubmittedSequenceNumber = new AtomicLong(-1);
    private final AtomicLong mLastCommittedSequenceNumber = new AtomicLong(-1);

    private final AtomicLong mCurrentCounter = new AtomicLong(0);

    private static final String JOURNAL_ENTRY_COUNT_MAX =
        "journal.entry.count.max";
    private static long maxBatchSize;
    private Pair<OMRequest, CompletableFuture<OMResponse>> entry;

    public RaftJournalWriter(OzoneManagerRatisServer ratisServer) {
      this.ratisServer = ratisServer;
      this.maxBatchSize =
          Long.parseLong(conf.get(JOURNAL_ENTRY_COUNT_MAX, "5"));
    }


    public void write(Pair<OMRequest, CompletableFuture<OMResponse>> pair) throws IOException {
//      LOG.info("write entry {}", entry.getKey().getCmdType());
      if (mClosed) {
        throw new IOException("Writer has been closed");
      }

      if (oMRequestsBuilder == null) {
        oMRequestsBuilder = OMRequest.newBuilder().setClientId("Client-12345");
      }
      long nextId = mNextSequenceNumberToWrite.getAndIncrement();
      oMRequestsBuilder.addRequests(pair.getKey().toBuilder()
          .setSequenceNumber(nextId)
          .build());
//      LOG.info("write SequenceNumber {}", nextId);
      responseFutures.put(nextId, pair.getValue());
      if (oMRequestsBuilder.getRequestsCount() > maxBatchSize) {
//        LOG.info("max batch {}", maxBatchSize);
        flush();
      }
    }

    public void flush() throws IOException {
      if (mClosed) {
        throw new IOException("Writer has been closed");
      }
      if (oMRequestsBuilder != null) {
        long flushSN = mNextSequenceNumberToWrite.get() - 1;
        try {
          mLastSubmittedSequenceNumber.set(flushSN);
          oMRequestsBuilder.setCmdType(Type.Requests)
              .setVersion(ClientVersion.CURRENT_VERSION)
              .setClientId(ClientId.randomId().toString());
          OMRequest omRequests = oMRequestsBuilder.build();
          CompletableFuture<RaftClientReply> future = ratisServer.submitRequests(omRequests);
          futures.add(future);
          mLastCommittedSequenceNumber.set(flushSN);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
        oMRequestsBuilder = null;
      }
    }


  }

  /**
   * Used to manage and keep track of pending callers of ::flush.
   */
  private static class FlushTicket implements ForkJoinPool.ManagedBlocker {
    private final long mTargetCounter;
    private final SettableFuture<Void> mIsCompleted;
    private Throwable mError;
    FlushTicket(long targetCounter) {
      mTargetCounter = targetCounter;
      mIsCompleted = SettableFuture.create();
      mError = null;
    }

    public long getTargetCounter() {
      return mTargetCounter;
    }

    public void setCompleted() {
      mIsCompleted.set(null);
    }

    public void setError(Throwable exc) {
      mIsCompleted.setException(exc);
      mError = exc;
    }

    /**
     * Waits until the ticket has been processed.
     *
     * PS: Blocking on this method goes through {@link ForkJoinPool}'s managed blocking
     * in order to compensate the pool with more workers while it is blocked.
     *
     * @throws Throwable error
     */
    public void waitCompleted() throws Throwable {
      ForkJoinPoolHelper.safeManagedBlock(this);
      if (mError != null) {
        throw mError;
      }
    }

    @Override
    public boolean block() throws InterruptedException {
      try {
        mIsCompleted.get();
      } catch (ExecutionException exc) {
        mError = exc.getCause();
      }
      return true;
    }

    @Override
    public boolean isReleasable() {
      return mIsCompleted.isDone() || mIsCompleted.isCancelled();
    }
  }

  @SuppressWarnings("checkstyle:HideUtilityClassConstructor")
  private static class ForkJoinPoolHelper {
    /**
     * Does managed blocking on ForkJoinPool. This helper is guaranteed to block even when
     * ForkJoinPool is running on full capacity.
     *
     * @param blocker managed blocker resource
     *
     * @throws InterruptedException
     */
    @SuppressWarnings({"checkstyle:NeedBraces", "checkstyle:EmptyStatement",
        "checkstyle:EmptyBlock"})
    public static void safeManagedBlock(ForkJoinPool.ManagedBlocker blocker)
        throws InterruptedException {
      try {
        ForkJoinPool.managedBlock(blocker);
      } catch (RejectedExecutionException re) {
        LOG.warn(
            "Failed to compensate rpc pool. Consider increasing thread pool size.",
            re);
        // Fall back to regular block on given blocker.
        while (!blocker.isReleasable() && !blocker.block()) { };
      }
    }
  }

}
