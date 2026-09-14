/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request.migrationKey;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus.PENDING;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.migrationKey.OMMigrationKeyDBUpdateResponse;
import org.apache.hadoop.ozone.om.response.migrationKey.OMMigrationKeyDBUpdateResponse.DBUpdateResult;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.AddTransaction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.CompleteTransaction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyDBUpdateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyDBUpdateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic database update request handler for migration key.
 */
public class OMMigrationKeyDBUpdateRequest extends OMClientRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMMigrationKeyDBUpdateRequest.class);

  public OMMigrationKeyDBUpdateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    MigrationKeyDBUpdateRequest request = getOmRequest().getMigrationKeyDBUpdateRequest();
    MigrationKeyArgs migrationKeyArgs = request.getMigrationKeyArgs();
    String taskKey = migrationKeyArgs.getTaskKey();
    switch (request.getType()) {
    case KEY_MIGRATION_COMPLETE_TRANSACTION:
      if (!migrationKeyArgs.hasCompleteTransaction()) {
        throw new OMException("complete transaction args missing for the key: " + taskKey,
            OMException.ResultCodes.INVALID_REQUEST);
      }
      CompleteTransaction completeTransaction = migrationKeyArgs.getCompleteTransaction();
      if (completeTransaction.getFailedKeyCount() < 0) {
        throw new IOException("Invalid complete transaction args for the failed key count: " +
            completeTransaction.getFailedKeyCount());
      }
      if (!MigrationTaskManager.extractTaskKeyFromTransactionKey(
          completeTransaction.getTransactionKey()).equals(taskKey)) {
        throw new IOException("Invalid transaction, the transaction" +
            completeTransaction.getTransactionKey() + "does not belong to the task " + taskKey);
      }
      break;
    case KEY_MIGRATION_UPDATE_TASK_STATUS:
      if (!migrationKeyArgs.hasUpdateTaskStatus()) {
        throw new OMException("update task args missing for the key: " + taskKey,
            OMException.ResultCodes.INVALID_REQUEST);
      }
      break;
    case KEY_MIGRATION_ADD_TRANSACTION:
      if (!migrationKeyArgs.hasAddTransaction()) {
        throw new OMException("add transaction args missing for the key: " + taskKey,
            OMException.ResultCodes.INVALID_REQUEST);
      }
      JobworkerMigrationKeysTxProto txProto =
          migrationKeyArgs.getAddTransaction().getMigrationKeysTxProto();
      if (!txProto.getTaskKey().equals(taskKey)) {
        throw new IOException("Invalid transaction, the transaction for task" +
            txProto.getTaskKey() + "does not belong to the task " + taskKey);
      }
      break;
    case KEY_MIGRATION_CREATE_TASK:
      if (!migrationKeyArgs.hasCreateTask()) {
        throw new OMException("create task args missing for the key: " + taskKey,
            OMException.ResultCodes.INVALID_REQUEST);
      }
      break;
    case KEY_MIGRATION_MARK_SCANNING_COMPLETED:
    case KEY_MIGRATION_CLEANUP_TASK:
      break;
    default:
      throw new OMException("Unsupported operation type: " +
          getOmRequest().getMigrationKeyDBUpdateRequest().getType(),
          OMException.ResultCodes.INVALID_REQUEST);
    }

    long operationTime = Time.now();
    MigrationKeyDBUpdateRequest updatedRequest = request.toBuilder()
        .setMigrationKeyArgs(migrationKeyArgs.toBuilder()
            .setOperationTime(operationTime)
            .build())
        .build();
    
    return getOmRequest().toBuilder()
        .setUserInfo(getUserInfo())
        .setMigrationKeyDBUpdateRequest(updatedRequest)
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    long transactionLogIndex = context.getIndex();

    MigrationKeyDBUpdateRequest request = getOmRequest().getMigrationKeyDBUpdateRequest();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMResponse.Builder omResponseBuilder = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMClientResponse omClientResponse = null;

    try {
      // Process operation and compute results
      DBUpdateResult dbUpdateResult = processOperation(omMetadataManager, request, transactionLogIndex);

      omResponseBuilder.setMigrationKeyDBUpdateResponse(MigrationKeyDBUpdateResponse.newBuilder().build())
          .setStatus(OK)
          .setSuccess(true);

      // Pass computed results to response
      omClientResponse = new OMMigrationKeyDBUpdateResponse(omResponseBuilder.build(), dbUpdateResult);
      LOG.debug("Successfully processed {} database operations in cache",
          request.getType());
    } catch (Exception ex) {
      // Build error response
      omClientResponse = new OMMigrationKeyDBUpdateResponse(
          createErrorOMResponse(omResponseBuilder, ex));
      LOG.error("Failed to process database update operations", ex);
    }

    return omClientResponse;
  }

  /**
   * Process a single database operation, update cache, and return computed results for DB writes.
   */
  private DBUpdateResult processOperation(OMMetadataManager omMetadataManager,
      MigrationKeyDBUpdateRequest request, long transactionLogIndex) throws Exception {
    MigrationKeyArgs migrationArgs = request.getMigrationKeyArgs();
    Preconditions.checkArgument(migrationArgs.getOperationTime() > 0);

    switch (request.getType()) {
    case KEY_MIGRATION_COMPLETE_TRANSACTION:
      return processCompleteMigrationTx(omMetadataManager, migrationArgs, transactionLogIndex);
    case KEY_MIGRATION_UPDATE_TASK_STATUS:
      return processUpdateMigrationTaskStatus(omMetadataManager, migrationArgs, transactionLogIndex);
    case KEY_MIGRATION_MARK_SCANNING_COMPLETED:
      return processMarkScanningCompleted(omMetadataManager, migrationArgs, transactionLogIndex);
    case KEY_MIGRATION_CLEANUP_TASK:
      return processCleanupMigrationTask(omMetadataManager, migrationArgs, transactionLogIndex);
    case KEY_MIGRATION_CREATE_TASK:
      return processCreateTask(omMetadataManager, migrationArgs, transactionLogIndex);
    case KEY_MIGRATION_ADD_TRANSACTION:
      return processAddTransaction(omMetadataManager, migrationArgs, transactionLogIndex);
    default:
      throw new OMException("Unsupported operation type: " + request.getType(),
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  private DBUpdateResult processCompleteMigrationTx(OMMetadataManager omMetadataManager,
      MigrationKeyArgs migrationKeyArgs, long transactionLogIndex)
      throws Exception {
    String taskKey = migrationKeyArgs.getTaskKey();
    String transactionKey = migrationKeyArgs.getCompleteTransaction().getTransactionKey();
    int failedKeyCount = migrationKeyArgs.getCompleteTransaction().getFailedKeyCount();
    
    Table<String, JobworkerMigrationKeysTaskProto> taskTable =
        omMetadataManager.getJobworkerMigrationKeysTaskTable();
    Table<String, JobworkerMigrationKeysTxProto> transactionTable =
        omMetadataManager.getJobworkerMigrationKeysTxTable();
    JobworkerMigrationKeysTxProto txProto = transactionTable.get(transactionKey);
    if (txProto == null) {
      throw new OMException("Transaction not found for key: " + transactionKey,
          OMException.ResultCodes.INVALID_REQUEST);
    }
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask == null) {
      throw new OMException("Migration task not found for key: " + taskKey,
          OMException.ResultCodes.INVALID_REQUEST);
    }
    // The number of successful keys is calculated as (total keys - failed keys).
    // This is because failed keys will be retried in subsequent commands, and only
    // the remaining failed keys are sent again. For example, if a transaction originally
    // contains 10 keys and 5 fail, the retry command will only include those 5 failed keys.
    // If all retries succeed, the final failedKeyCount will be 0, and the total number of
    // successful keys will be 10. Thus, by tracking only the final failedKeyCount, we can
    // always determine the number of successful keys as (total - failed).
    long successCount = txProto.getMigrationKeysCount() - failedKeyCount;
    JobworkerMigrationKeysTaskProto.Builder builder = currentTask.toBuilder();
    builder.setMigratedKeyCount(builder.getMigratedKeyCount() + successCount);
    builder.setFailedKeyCount(builder.getFailedKeyCount() + failedKeyCount);
    builder.setLastUpdateTime(migrationKeyArgs.getOperationTime());
    JobworkerMigrationKeysTaskProto updatedTask = builder.build();
    
    transactionTable.addCacheEntry(new CacheKey<>(transactionKey),
        CacheValue.get(transactionLogIndex));
    taskTable.addCacheEntry(new CacheKey<>(taskKey),
        CacheValue.get(transactionLogIndex, updatedTask));

    LOG.debug("Updated migration task cache for {}: +{} succeeded, +{} failed",
        taskKey, successCount, failedKeyCount);
    return DBUpdateResult.createMigrationKeyCompleteTxResult(
        taskKey, transactionKey, updatedTask);
  }

  private DBUpdateResult processUpdateMigrationTaskStatus(OMMetadataManager omMetadataManager,
      MigrationKeyArgs migrationKeyArgs, long transactionLogIndex) throws Exception {
    String taskKey = migrationKeyArgs.getTaskKey();
    JobworkerTaskStatus newStatus = migrationKeyArgs.getUpdateTaskStatus().getNewStatus();

    Table<String, JobworkerMigrationKeysTaskProto> taskTable =
        omMetadataManager.getJobworkerMigrationKeysTaskTable();
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask == null) {
      throw new OMException("Migration task not found for key: " + taskKey,
          OMException.ResultCodes.INVALID_REQUEST);
    }
    JobworkerMigrationKeysTaskProto.Builder builder = currentTask.toBuilder();
    builder.setMigrationStatus(newStatus);
    builder.setLastUpdateTime(migrationKeyArgs.getOperationTime());
    JobworkerMigrationKeysTaskProto updatedTask = builder.build();

    taskTable.addCacheEntry(new CacheKey<>(taskKey),
        CacheValue.get(transactionLogIndex, updatedTask));

    LOG.debug("Updated migration status to {} for task: {}", newStatus, taskKey);
    return DBUpdateResult.createMigrationUpdateTaskStatusResult(taskKey, updatedTask);
  }

  private DBUpdateResult processMarkScanningCompleted(OMMetadataManager omMetadataManager,
      MigrationKeyArgs migrationKeyArgs, long transactionLogIndex) throws Exception {

    String taskKey = migrationKeyArgs.getTaskKey();

    Table<String, JobworkerMigrationKeysTaskProto> taskTable =
        omMetadataManager.getJobworkerMigrationKeysTaskTable();
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask == null) {
      throw new OMException("Migration task not found for key: " + taskKey,
          OMException.ResultCodes.INVALID_REQUEST);
    }
    
    JobworkerMigrationKeysTaskProto.Builder builder = currentTask.toBuilder();
    builder.setCompleteScanning(true);
    builder.setLastUpdateTime(migrationKeyArgs.getOperationTime());
    JobworkerMigrationKeysTaskProto updatedTask = builder.build();

    taskTable.addCacheEntry(new CacheKey<>(taskKey),
        CacheValue.get(transactionLogIndex, updatedTask));

    LOG.debug("Marked scanning as completed for the task: {}", taskKey);
    return DBUpdateResult.createMigrationMarkScanningCompletedResult(taskKey, updatedTask);
  }

  private DBUpdateResult processCleanupMigrationTask(OMMetadataManager omMetadataManager,
      MigrationKeyArgs migrationKeyArgs, long transactionLogIndex) throws Exception {

    String taskKey = migrationKeyArgs.getTaskKey();

    Table<String, JobworkerMigrationKeysTaskProto> taskTable =
        omMetadataManager.getJobworkerMigrationKeysTaskTable();
    
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask == null) {
      throw new OMException("Migration task not found for key: " + taskKey,
          OMException.ResultCodes.INVALID_REQUEST);
    }
    
    taskTable.addCacheEntry(new CacheKey<>(taskKey),
        CacheValue.get(transactionLogIndex));

    LOG.debug("Mark the migration task for cleanup: {}", taskKey);
    return DBUpdateResult.createMigrationCleanupTaskResult(taskKey);
  }

  private DBUpdateResult processCreateTask(OMMetadataManager omMetadataManager,
      MigrationKeyArgs migrationKeyArgs, long transactionLogIndex) throws Exception {

    String taskKey = migrationKeyArgs.getTaskKey();

    Table<String, JobworkerMigrationKeysTaskProto> taskTable =
        omMetadataManager.getJobworkerMigrationKeysTaskTable();
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask != null) {
      throw new OMException("The migration task " + taskKey + " already exists. ",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    JobworkerMigrationKeysTaskProto migrationKeysTaskProto = JobworkerMigrationKeysTaskProto
        .newBuilder()
        .setMigrationStatus(PENDING)
        .setTotalKeyCount(0)
        .setMigratedKeyCount(0)
        .setFailedKeyCount(0)
        .setStartTime(migrationKeyArgs.getOperationTime())
        .setLastUpdateTime(migrationKeyArgs.getOperationTime())
        .setCompleteScanning(false)
        .setRuleId(migrationKeyArgs.getCreateTask().getRuleId())
        .setEcReplicationConfig(migrationKeyArgs.getCreateTask().getEcReplicationConfig())
        .build();

    taskTable.addCacheEntry(new CacheKey<>(taskKey),
        CacheValue.get(transactionLogIndex, migrationKeysTaskProto));

    LOG.debug("create a new migration task {}", taskKey);
    return DBUpdateResult.createMigrationCreateTaskResult(taskKey, migrationKeysTaskProto);
  }

  private DBUpdateResult processAddTransaction(OMMetadataManager omMetadataManager,
      MigrationKeyArgs migrationKeyArgs, long transactionLogIndex) throws Exception {

    String taskKey = migrationKeyArgs.getTaskKey();
    Table<String, JobworkerMigrationKeysTaskProto> taskTable =
        omMetadataManager.getJobworkerMigrationKeysTaskTable();
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask == null) {
      throw new OMException("Migration task not found for key: " + taskKey,
          OMException.ResultCodes.INVALID_REQUEST);
    }

    AddTransaction addTransactionArgs = migrationKeyArgs.getAddTransaction();
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, addTransactionArgs.getTxId());
    Table<String, JobworkerMigrationKeysTxProto> txTable =
        omMetadataManager.getJobworkerMigrationKeysTxTable();
    JobworkerMigrationKeysTxProto txProto = txTable.get(transactionKey);
    if (txProto != null) {
      throw new OMException("The migration transaction " + transactionKey +
          " for task " + taskKey + " already exists. ", OMException.ResultCodes.INVALID_REQUEST);
    }
    JobworkerMigrationKeysTxProto newTxProto = addTransactionArgs.getMigrationKeysTxProto();
    JobworkerMigrationKeysTaskProto updatedTask = currentTask.toBuilder()
        .setLastUpdateTime(migrationKeyArgs.getOperationTime())
        .setTotalKeyCount(currentTask.getTotalKeyCount() + newTxProto.getMigrationKeysCount())
        .build();

    taskTable.addCacheEntry(new CacheKey<>(taskKey), CacheValue.get(transactionLogIndex, updatedTask));
    txTable.addCacheEntry(new CacheKey<>(transactionKey), CacheValue.get(transactionLogIndex, newTxProto));

    LOG.debug("create a new migration transaction {} for task {}", transactionKey, taskKey);
    return DBUpdateResult.createMigrationAddTransactionResult(
        taskKey, transactionKey, updatedTask, newTxProto);
  }
}
