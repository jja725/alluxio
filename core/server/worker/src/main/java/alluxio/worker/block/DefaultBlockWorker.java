/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import static alluxio.worker.block.BlockMetadataManager.WORKER_STORAGE_TIER_ASSOC;
import static java.lang.String.format;

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.Sessions;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.PrefixList;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.WorkerOutOfSpaceRuntimeException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.FileBlocks;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryUtils;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsManager;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.grpc.GrpcExecutors;
import alluxio.worker.page.PagedLocalBlockStore;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.Closer;
import org.apache.ratis.thirdparty.com.google.rpc.RetryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This includes:
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link DefaultBlockWorker} (Logic for all block related storage operations)
 */
@NotThreadSafe
public class DefaultBlockWorker extends AbstractWorker implements BlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockWorker.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /** Used to close resources during stop. */
  private final Closer mResourceCloser = Closer.create();
  /**
   * Block master clients. commitBlock is the only reason to keep a pool of block master clients
   * on each worker. We should either improve our RPC model in the master or get rid of the
   * necessity to call commitBlock in the workers.
   */
  private final BlockMasterClientPool mBlockMasterClientPool;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  /** Block store delta reporter for master heartbeat. */
  private final BlockHeartbeatReporter mHeartbeatReporter;
  /** Session metadata, used to keep track of session heartbeats. */
  private final Sessions mSessions;
  /** Block Store manager. */
  private final LocalBlockStore mLocalBlockStore;
  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  /** The under file system block store. */
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;

  /**
   * The worker ID for this worker. This is initialized in {@link #start(WorkerNetAddress)} and may
   * be updated by the block sync thread if the master requests re-registration.
   */
  private final AtomicReference<Long> mWorkerId;

  private final CacheRequestManager mCacheManager;
  private final FuseManager mFuseManager;

  private WorkerNetAddress mAddress;

  /**
   * Constructs a default block worker.
   *
   * @param ufsManager ufs manager
   */
  DefaultBlockWorker(UfsManager ufsManager) {
    this(new BlockMasterClientPool(),
        new FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(Configuration.global())).build()),
        new Sessions(), LocalBlockStore.create(ufsManager), ufsManager);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param ufsManager ufs manager
   */
  @VisibleForTesting
  public DefaultBlockWorker(BlockMasterClientPool blockMasterClientPool,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions, LocalBlockStore blockStore,
      UfsManager ufsManager) {
    super(ExecutorServiceFactories.fixedThreadPool("block-worker-executor", 5));
    mBlockMasterClientPool = mResourceCloser.register(blockMasterClientPool);
    mFileSystemMasterClient = mResourceCloser.register(fileSystemMasterClient);
    mHeartbeatReporter = new BlockHeartbeatReporter();
    /* Metrics reporter that listens on block events and increases metrics counters. */
    BlockMetricsReporter metricsReporter = new BlockMetricsReporter();
    mSessions = sessions;
    mLocalBlockStore = mResourceCloser.register(blockStore);
    mWorkerId = new AtomicReference<>(-1L);
    mLocalBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mLocalBlockStore.registerBlockStoreEventListener(metricsReporter);
    FileSystemContext fsContext = mResourceCloser.register(
        FileSystemContext.create(ClientContext.create(Configuration.global()), this));
    mUnderFileSystemBlockStore = new UnderFileSystemBlockStore(mLocalBlockStore, ufsManager);
    mCacheManager = new CacheRequestManager(
        GrpcExecutors.CACHE_MANAGER_EXECUTOR, this, fsContext);
    mFuseManager = mResourceCloser.register(new FuseManager(fsContext));
    mWhitelist = new PrefixList(Configuration.getList(PropertyKey.WORKER_WHITELIST));

    Metrics.registerGauges(this);
  }

  /**
   * get the LocalBlockStore that manages local blocks.
   *
   * @return the LocalBlockStore that manages local blocks
   * */
  public LocalBlockStore getLocalBlockStore() {
    return mLocalBlockStore;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return Collections.emptyMap();
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return mWorkerId;
  }

  /**
   * Runs the block worker. The thread must be called after all services (e.g., web, dataserver)
   * started.
   *
   * BlockWorker doesn't support being restarted!
   */
  @Override
  public void start(WorkerNetAddress address) throws IOException {
    super.start(address);
    mAddress = address;

    // Acquire worker Id.
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      RetryUtils.retry("create worker id", () -> mWorkerId.set(blockMasterClient.getId(address)),
          RetryUtils.defaultWorkerMasterClientRetry(Configuration
              .getDuration(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a worker id from block master: "
          + e.getMessage());
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }

    Preconditions.checkNotNull(mWorkerId, "mWorkerId");
    Preconditions.checkNotNull(mAddress, "mAddress");

    // Setup BlockMasterSync
    BlockMasterSync blockMasterSync = mResourceCloser
        .register(new BlockMasterSync(this, mWorkerId, mAddress, mBlockMasterClientPool));
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, blockMasterSync,
            (int) Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
            Configuration.global(), ServerUserState.global()));

    // Setup PinListSyncer
    PinListSync pinListSync = mResourceCloser.register(
        new PinListSync(this, mFileSystemMasterClient));
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, pinListSync,
            (int) Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
            Configuration.global(), ServerUserState.global()));

    // Setup session cleaner
    SessionCleaner sessionCleaner = mResourceCloser
        .register(new SessionCleaner(mSessions, mLocalBlockStore, mUnderFileSystemBlockStore));
    getExecutorService().submit(sessionCleaner);

    // Setup storage checker
    if (Configuration.getBoolean(PropertyKey.WORKER_STORAGE_CHECKER_ENABLED)) {
      StorageChecker storageChecker = mResourceCloser.register(new StorageChecker());
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.WORKER_STORAGE_HEALTH, storageChecker,
              (int) Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
                  Configuration.global(), ServerUserState.global()));
    }

    // Mounts the embedded Fuse application
    if (Configuration.getBoolean(PropertyKey.WORKER_FUSE_ENABLED)) {
      mFuseManager.start();
    }
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   *
   * BlockWorker doesn't support being restarted!
   */
  @Override
  public void stop() throws IOException {
    // Stop the base. (closes executors.)
    // This is intentionally called first in order to send interrupt signals to heartbeat threads.
    // Otherwise, if the heartbeat threads are not interrupted then the shutdown can hang.
    super.stop();
    // Stop heart-beat executors and clients.
    mResourceCloser.close();
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws IOException {
    mLocalBlockStore.abortBlock(sessionId, blockId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws IOException {
    OptionalLong lockId = OptionalLong.of(
        mLocalBlockStore.commitBlockLocked(sessionId, blockId, pinOnCreate));

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      BlockMeta meta = mLocalBlockStore.getVolatileBlockMeta(blockId).get();
      BlockStoreLocation loc = meta.getBlockLocation();
      blockMasterClient.commitBlock(mWorkerId.get(),
          mLocalBlockStore.getBlockStoreMeta().getUsedBytesOnTiers().get(loc.tierAlias()),
          loc.tierAlias(), loc.mediumType(), blockId, meta.getBlockSize());
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
      if (lockId.isPresent()) {
        mLocalBlockStore.unpinBlock(lockId.getAsLong());
      }
      Metrics.WORKER_ACTIVE_CLIENTS.dec();
    }
  }

  @Override
  public void commitBlockInUfs(long blockId, long length) throws IOException {
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      blockMasterClient.commitBlockInUfs(blockId, length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions)
      throws WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc;
    String tierAlias = WORKER_STORAGE_TIER_ASSOC.getAlias(tier);
    if (Strings.isNullOrEmpty(createBlockOptions.getMedium())) {
      loc = BlockStoreLocation.anyDirInTier(tierAlias);
    } else {
      loc = BlockStoreLocation.anyDirInAnyTierWithMedium(createBlockOptions.getMedium());
    }
    TempBlockMeta createdBlock;
    try {
      createdBlock = mLocalBlockStore.createBlock(sessionId, blockId,
          AllocateOptions.forCreate(createBlockOptions.getInitialBytes(), loc));
    } catch (WorkerOutOfSpaceRuntimeException e) {
      LOG.error(
          "Failed to create block. SessionId: {}, BlockId: {}, "
              + "TierAlias:{}, Medium:{}, InitialBytes:{}, Error:{}",
          sessionId, blockId, tierAlias,
          createBlockOptions.getMedium(), createBlockOptions.getInitialBytes(), e);

      InetSocketAddress address =
          InetSocketAddress.createUnresolved(mAddress.getHost(), mAddress.getRpcPort());
      throw new WorkerOutOfSpaceRuntimeException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, address, blockId), e);
    }
    Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return createdBlock.getPath();
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException {
    return mLocalBlockStore.createBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return mLocalBlockStore.getBlockStoreMeta();
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return mLocalBlockStore.getBlockStoreMetaFull();
  }

  @Override
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    try {
      BlockReader reader = mUnderFileSystemBlockStore.createBlockReader(sessionId, blockId, offset,
          positionShort, options);
      return new DelegatingBlockReader(reader, () -> closeUfsBlock(sessionId, blockId));
    } catch (Exception e) {
      try {
        closeUfsBlock(sessionId, blockId);
      } catch (Exception ee) {
        LOG.warn("Failed to close UFS block", ee);
      }
      throw new UnavailableException(format("Failed to read from UFS, sessionId=%d, "
              + "blockId=%d, offset=%d, positionShort=%s, options=%s: %s",
          sessionId, blockId, offset, positionShort, options, e), e);
    }
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws IOException {
    mLocalBlockStore.removeBlock(sessionId, blockId);
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws WorkerOutOfSpaceException, IOException {
    mLocalBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public void asyncCache(AsyncCacheRequest request) {
    CacheRequest cacheRequest =
        CacheRequest.newBuilder().setBlockId(request.getBlockId()).setLength(request.getLength())
            .setOpenUfsBlockOptions(request.getOpenUfsBlockOptions())
            .setSourceHost(request.getSourceHost()).setSourcePort(request.getSourcePort())
            .setAsync(true).build();
    try {
      mCacheManager.submitRequest(cacheRequest);
    } catch (Exception e) {
      LOG.warn("Failed to submit async cache request. request: {}", request, e);
    }
  }

  @Override
  public void cache(CacheRequest request) throws AlluxioException, IOException {
    mCacheManager.submitRequest(request);
  }

  @Override
  public List<BlockStatus> load(List<FileBlocks> fileBlocks, String tag, long bandwidth) {
    ArrayList<BlockStatus> failures = new ArrayList<>();
    ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
    for (FileBlocks blocks : fileBlocks) {
      for (long blockId : blocks.getBlockIdList()) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(
            () -> loadInternal(blocks.getUfsPath(), blockId, blocks.getBlockSize(), tag, bandwidth),
            GrpcExecutors.BLOCK_READER_EXECUTOR);
        future.exceptionally((throwable) -> {
          failures.add(BlockStatus.newBuilder().setBlockId(blockId).setCode(1)
              .setMessage(throwable.getMessage()).setRetryable(false).build());
          return null;
        });
        futures.add(future);
      }
    }
    futures.forEach(CompletableFuture::join);
    return failures;
  }

  private void loadInternal(String ufsPath, long blockId, long blockSize, String tag,
      long bandwidth) {
    long sessionId = IdUtils.createSessionId();
    try {
      mLocalBlockStore.requestSpace(sessionId, blockId, blockSize);
      BlockStoreLocation loc =
          BlockStoreLocation.anyDirInTier(WORKER_STORAGE_TIER_ASSOC.getAlias(0));
      mLocalBlockStore.createBlock(sessionId, blockId, AllocateOptions.forCreate(blockSize, loc));
    } catch (IOException e) {
      throw new AlluxioRuntimeException("Failed to create block when loading: %s", e);
    }

    try (BlockWriter blockWriter = mLocalBlockStore.createBlockWriter(sessionId, blockId)) {
      long offset = 0;
      while (offset < blockSize) {
        long bufferSize = Math.min(8L * Constants.MB, blockSize - offset);
        byte[] data = read(blockId, blockSize, ufsPath, 0, bufferSize, false, tag);
        offset += bufferSize;
        ByteBuffer buffer = ByteBuffer.wrap(data, (int) (blockWriter.getPosition() - offset),
            (int) (offset - blockWriter.getPosition()));
        blockWriter.append(buffer);
      }
      commitBlock(sessionId, blockId, false);
    } catch (IOException e) {
      try {
        mLocalBlockStore.abortBlock(sessionId, blockId);
      } catch (IOException ee) {
        LOG.error("Failed to abort block after failing block load task:", ee);
      }
      throw new AlluxioRuntimeException(format("Failed to load data from UFS: %s", blockId), e);
    }
  }

  private byte[] read(long blockId, long blockSize, String ufsPath, long offset, long length,
      boolean positionShort, String tag) {
    return new byte[0];
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    mLocalBlockStore.updatePinnedInodes(pinnedInodes);
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return mFileSystemMasterClient.getFileInfo(fileId);
  }

  /**
   * Closes a UFS block for a client session. It also commits the block to Alluxio block store
   * if the UFS block has been cached successfully.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   */
  @VisibleForTesting
  public void closeUfsBlock(long sessionId, long blockId)
      throws IOException {
    try {
      mUnderFileSystemBlockStore.close(sessionId, blockId);
      Optional<TempBlockMeta> tempBlockMeta = mLocalBlockStore.getTempBlockMeta(blockId);
      if (tempBlockMeta.isPresent() && tempBlockMeta.get().getSessionId() == sessionId) {
        commitBlock(sessionId, blockId, false);
      } else {
        // When getTempBlockMeta() return null, such as a block readType NO_CACHE writeType THROUGH.
        // Counter will not be decrement in the commitblock().
        // So we should decrement counter here.
        if (mUnderFileSystemBlockStore.isNoCache(sessionId, blockId)) {
          Metrics.WORKER_ACTIVE_CLIENTS.dec();
        }
      }
    } finally {
      mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
    }
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    BlockReader reader;
    if (mLocalBlockStore instanceof PagedLocalBlockStore) {
      reader = mLocalBlockStore.createBlockReader(sessionId, blockId, options);
      Metrics.WORKER_ACTIVE_CLIENTS.inc();
      return reader;
    }
    Optional<BlockMeta> blockMeta = mLocalBlockStore.getVolatileBlockMeta(blockId);
    if (blockMeta.isPresent()) {
      reader = mLocalBlockStore.createBlockReader(sessionId, blockId, offset);
    } else {
      boolean checkUfs = options != null && (options.hasUfsPath() || options.getBlockInUfsTier());
      if (!checkUfs) {
        throw new BlockDoesNotExistRuntimeException(blockId);
      }
      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      reader = createUfsBlockReader(sessionId, blockId, offset, positionShort, options);
    }
    Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return reader;
  }

  @Override
  public void clearMetrics() {
    // TODO(lu) Create a metrics worker and move this method to metrics worker
    MetricsSystem.resetAllMetrics();
  }

  @Override
  public alluxio.wire.Configuration getConfiguration(GetConfigurationPOptions options) {
    // NOTE(cc): there is no guarantee that the returned cluster and path configurations are
    // consistent snapshot of the system's state at a certain time, the path configuration might
    // be in a newer state. But it's guaranteed that the hashes are respectively correspondent to
    // the properties.
    alluxio.wire.Configuration.Builder builder = alluxio.wire.Configuration.newBuilder();

    if (!options.getIgnoreClusterConf()) {
      for (PropertyKey key : Configuration.keySet()) {
        if (key.isBuiltIn()) {
          Source source = Configuration.getSource(key);
          Object value = Configuration.getOrDefault(key, null,
                  ConfigurationValueOptions.defaults().useDisplayValue(true)
                          .useRawValue(options.getRawValue()));
          builder.addClusterProperty(key.getName(), value, source);
        }
      }
      // NOTE(cc): assumes that Configuration is read-only when master is running, otherwise,
      // the following hash might not correspond to the above cluster configuration.
      builder.setClusterConfHash(Configuration.hash());
    }

    return builder.build();
  }

  @Override
  public void cleanupSession(long sessionId) {
    mLocalBlockStore.cleanupSession(sessionId);
    mUnderFileSystemBlockStore.cleanupSession(sessionId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  /**
   * This class contains some metrics related to the block worker.
   * This class is public because the metric names are referenced in
   * {@link alluxio.web.WebInterfaceAbstractMetricsServlet}.
   */
  @ThreadSafe
  public static final class Metrics {
    public static final Counter WORKER_ACTIVE_CLIENTS =
        MetricsSystem.counter(MetricKey.WORKER_ACTIVE_CLIENTS.getName());

    /**
     * Registers metric gauges.
     *
     * @param blockWorker the block worker handle
     */
    public static void registerGauges(final BlockWorker blockWorker) {
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_TOTAL.getName()),
          () -> blockWorker.getStoreMeta().getCapacityBytes());

      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_USED.getName()),
          () -> blockWorker.getStoreMeta().getUsedBytes());

      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_FREE.getName()),
          () -> blockWorker.getStoreMeta().getCapacityBytes() - blockWorker.getStoreMeta()
                      .getUsedBytes());

      for (int i = 0; i < WORKER_STORAGE_TIER_ASSOC.size(); i++) {
        String tier = WORKER_STORAGE_TIER_ASSOC.getAlias(i);
        // TODO(lu) Add template to dynamically generate MetricKey
        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + tier),
            () -> blockWorker.getStoreMeta().getCapacityBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + tier),
            () -> blockWorker.getStoreMeta().getUsedBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + tier),
            () -> blockWorker.getStoreMeta().getCapacityBytesOnTiers().getOrDefault(tier, 0L)
                - blockWorker.getStoreMeta().getUsedBytesOnTiers().getOrDefault(tier, 0L));
      }
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
          MetricKey.WORKER_BLOCKS_CACHED.getName()),
          () -> blockWorker.getStoreMetaFull().getNumberOfBlocks());
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * StorageChecker periodically checks the health of each storage path and report missing blocks to
   * {@link BlockWorker}.
   */
  @NotThreadSafe
  public final class StorageChecker implements HeartbeatExecutor {

    @Override
    public void heartbeat() {
      try {
        mLocalBlockStore.removeInaccessibleStorage();
      } catch (Exception e) {
        LOG.warn("Failed to check storage: {}", e.toString());
        LOG.debug("Exception: ", e);
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
