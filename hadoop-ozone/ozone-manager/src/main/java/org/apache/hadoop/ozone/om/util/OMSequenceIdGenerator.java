/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.util;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateIdBatchRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateIdBatchResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.CallId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OM-side sequence ID generator that supports HA failover and non-Ratis modes.
 * 
 * After OM starts, the sequence ID generator maintains batches of pre-allocated IDs
 * in memory for efficient ID generation. When a batch is exhausted, a new batch
 * is allocated from the OM metadata store through a distributed request.
 * 
 * In order to maintain monotonicity during HA failover, when becoming leader,
 * OM invalidates all un-exhausted ID batches, forcing the new leader to reload
 * the last ID from the metadata store and allocate fresh batches on the first
 * getNextId() call.
 */
public class OMSequenceIdGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(OMSequenceIdGenerator.class);
  private final ClientId clientId = ClientId.randomId();
  public static final long INVALID_SEQUENCE_ID = -1L;

  private final int batchSize;
  private final OzoneManager ozoneManager;
  private final HashMap<String, Batch> sequenceIdToBatchMap;
  private final ReentrantLock lock;

  /**
   * Ids supported.
   */
  public static final String MIGRATION_KEYS_TX_ID = "migrationKeysTxId";
  public static final String MIGRATION_KEYS_TASK_ID = "migrationKeysTaskId";

  /**
   * Represents a batch of allocated IDs.
   */
  private static class Batch {
    private final long startId; // inclusive
    private final long endId; // inclusive
    private final AtomicLong currentId;

    Batch(long startId, long endId) {
      this.startId = startId;
      this.endId = endId;
      this.currentId = new AtomicLong(startId);
      
      // Validate that startId and endId are valid
      if (startId < 0 || endId < startId) {
        throw new IllegalArgumentException(
            String.format("Invalid batch range: startId=%d, endId=%d", startId, endId));
      }
    }

    /**
     * Get the next ID from this batch.
     * @return next ID, or Batch.INVALID_SEQUENCE_ID if batch is exhausted
     */
    long getNextId() {
      if (isExhausted()) {
        return INVALID_SEQUENCE_ID; // Batch exhausted
      }
      return currentId.getAndIncrement();
    }

    /**
     * Check if this batch is exhausted.
     * @return true if exhausted
     */
    boolean isExhausted() {
      return currentId.get() > endId;
    }

    @Override
    public String toString() {
      return String.format("Batch[%d, %d), current=%d", startId, endId, currentId.get());
    }
  }

  public OMSequenceIdGenerator(ConfigurationSource conf, OzoneManager ozoneManager) {
    this.batchSize = conf.getInt(
        OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE,
        OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE_DEFAULT);
    this.ozoneManager = ozoneManager;
    this.sequenceIdToBatchMap = new HashMap<>();
    this.lock = new ReentrantLock();

    LOG.debug("Initialized OMSequenceIdGenerator with batch size: {} (clientId {})", batchSize, clientId);
  }

  /**
   * Gets the next sequence ID for the given sequence name.
   */
  public long getNextId(String sequenceIdName) throws ServiceException, IOException {
    lock.lock();
    try {
      Batch currentBatch = sequenceIdToBatchMap.get(sequenceIdName);

      // If no batch exists or current batch is exhausted, allocate a new batch
      if (currentBatch == null || currentBatch.isExhausted()) {
        allocateNewBatch(sequenceIdName);
        currentBatch = sequenceIdToBatchMap.get(sequenceIdName);
      }

      if (currentBatch == null) {
        throw new OMException("Failed to allocate batch for " + sequenceIdName,
            OMException.ResultCodes.INTERNAL_ERROR);
      }

      long nextId = currentBatch.getNextId();
      if (nextId == INVALID_SEQUENCE_ID) {
        // Batch exhausted, try to allocate a new one
        allocateNewBatch(sequenceIdName);
        currentBatch = sequenceIdToBatchMap.get(sequenceIdName);
        if (currentBatch != null) {
          nextId = currentBatch.getNextId();
        }
      }

      if (nextId == INVALID_SEQUENCE_ID) {
        throw new OMException("Failed to get next ID for " + sequenceIdName,
            OMException.ResultCodes.INTERNAL_ERROR);
      }

      return nextId;
    } finally {
      lock.unlock();
    }
  }

  private void allocateNewBatch(String sequenceIdName)
      throws ServiceException, IOException {
    Preconditions.checkArgument(lock.isLocked());
    Batch currentBatch = sequenceIdToBatchMap.get(sequenceIdName);
    if (currentBatch != null && !currentBatch.isExhausted()) {
      return; // Another thread already allocated a new batch
    }

    AllocateIdBatchRequest allocateIdBatchRequest = AllocateIdBatchRequest.newBuilder()
        .setSequenceIdName(sequenceIdName)
        .setBatchSize(batchSize)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(clientId.toString())
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setUserInfo(getUserInfo())
        .setAllocateIdBatchRequest(allocateIdBatchRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateIdBatch)
        .build();

    OMResponse omResponse = submitRequest(omRequest);
    
    if (omResponse.getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      AllocateIdBatchResponse response = omResponse.getAllocateIdBatchResponse();
      long startId = response.getStartId();
      long endId = response.getEndId();
      
      // Create new batch
      Batch newBatch = new Batch(startId, endId);
      sequenceIdToBatchMap.put(sequenceIdName, newBatch);
      
      LOG.debug("Allocated new batch for {} (clientId {}): [{}, {}) ",
          sequenceIdName, clientId, startId, endId);
    } else {
      throw new OMException("Failed to allocate batch: " + omResponse.getMessage(),
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  protected OMResponse submitRequest(OMRequest omRequest)
      throws ServiceException, IOException {
    return ozoneManager.getOmRatisServer().submitRequest(
        omRequest, clientId, CallId.getAndIncrement());
  }

  private OzoneManagerProtocolProtos.UserInfo getUserInfo() throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    InetAddress remoteAddress = ozoneManager.getOmRpcServerAddr().getAddress();
    OzoneManagerProtocolProtos.UserInfo.Builder userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder();
    if (user != null) {
      userInfo.setUserName(user.getUserName());
    }

    if (remoteAddress != null) {
      userInfo.setHostName(remoteAddress.getHostName());
      userInfo.setRemoteAddress(remoteAddress.getHostAddress());
    }

    return userInfo.build();
  }

  /**
   * Invalidate any un-exhausted batch, next getNextId() call will
   * allocate a new batch, should call during HA failover.
   */
  public void invalidateAllBatches() {
    lock.lock();
    try {
      sequenceIdToBatchMap.clear();
      LOG.info("Invalidated all sequence ID batches due to HA failover");
    } finally {
      lock.unlock();
    }
  }
}
