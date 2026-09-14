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

package org.apache.hadoop.ozone.om.response.migrationKey;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.JOBWORKER_MIGRATION_KEYS_TASK_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.JOBWORKER_MIGRATION_KEYS_TRANSACTION_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.request.migrationKey.OMMigrationKeyDBUpdateRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyOperationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for {@link OMMigrationKeyDBUpdateRequest} request.
 */
@CleanupTableInfo(cleanupTables = {JOBWORKER_MIGRATION_KEYS_TASK_TABLE,
    JOBWORKER_MIGRATION_KEYS_TRANSACTION_TABLE})
public class OMMigrationKeyDBUpdateResponse extends OMClientResponse {
  private static final Logger LOG = LoggerFactory.getLogger(OMMigrationKeyDBUpdateResponse.class);

  private DBUpdateResult dbUpdateResult;

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMMigrationKeyDBUpdateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  /**
   * For successful requests with computed results.
   */
  public OMMigrationKeyDBUpdateResponse(@Nonnull OMResponse omResponse,
      @Nonnull DBUpdateResult dbUpdateResult) {
    super(omResponse);
    Preconditions.checkNotNull(dbUpdateResult);
    Preconditions.checkNotNull(dbUpdateResult.operationData);
    switch (dbUpdateResult.getOperationType()) {
    case KEY_MIGRATION_COMPLETE_TRANSACTION:
    case KEY_MIGRATION_UPDATE_TASK_STATUS:
    case KEY_MIGRATION_MARK_SCANNING_COMPLETED:
    case KEY_MIGRATION_CLEANUP_TASK:
    case KEY_MIGRATION_CREATE_TASK:
    case KEY_MIGRATION_ADD_TRANSACTION:
    case KEY_MIGRATION_CANCEL_TASK:
    case KEY_MIGRATION_DELETE_TRANSACTIONS:
      Preconditions.checkArgument(dbUpdateResult.operationData instanceof MigrationOperationData);
      break;
    default:
      throw new UnsupportedOperationException("Unsupported operationType: " + dbUpdateResult.getOperationType());
    }
    this.dbUpdateResult = dbUpdateResult;
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getStatus() == OK) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    switch (dbUpdateResult.getOperationType()) {
    case KEY_MIGRATION_COMPLETE_TRANSACTION:
      executeMigrationCompleteTransaction(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_UPDATE_TASK_STATUS:
      executeMigrationUpdateTaskStatus(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_MARK_SCANNING_COMPLETED:
      executeMigrationMarkScanningCompleted(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_CLEANUP_TASK:
      executeMigrationCleanupTask(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_CREATE_TASK:
      executeMigrationCreateTask(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_ADD_TRANSACTION:
      executeMigrationAddTransaction(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_CANCEL_TASK:
      executeMigrationUpdateTaskStatus(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    case KEY_MIGRATION_DELETE_TRANSACTIONS:
      executeMigrationDeleteTransactions(omMetadataManager, batchOperation, dbUpdateResult);
      break;
    default:
      // This should never happen due to constructor validation, but keep for safety
      throw new IOException("Unsupported operation type: " + dbUpdateResult.getOperationType());
    }
    LOG.debug("Successfully executed {} database operations", dbUpdateResult.getOperationType());
  }

  private void executeMigrationCompleteTransaction(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();

    omMetadataManager.getJobworkerMigrationKeysTxTable()
        .deleteWithBatch(batchOperation, migrationData.getTransactionKey());
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, migrationData.getTaskKey(), migrationData.getTask());

    LOG.debug("Executed migration complete transaction DB operations for task: {}", migrationData.getTaskKey());
  }

  private void executeMigrationUpdateTaskStatus(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();

    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, migrationData.getTaskKey(), migrationData.getTask());

    LOG.debug("Executed migration update task status DB operations for task: {}", migrationData.getTaskKey());
  }

  private void executeMigrationMarkScanningCompleted(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();

    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, migrationData.getTaskKey(), migrationData.getTask());

    LOG.debug("Executed migration mark scanning completed DB operations for task: {}", migrationData.getTaskKey());
  }

  private void executeMigrationCleanupTask(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();

    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .deleteWithBatch(batchOperation, migrationData.getTaskKey());

    LOG.debug("Executed migration cleanup task DB operations for task: {}", migrationData.getTaskKey());
  }

  private void executeMigrationCreateTask(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();

    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, migrationData.getTaskKey(), migrationData.getTask());

    LOG.debug("Executed migration creates task DB operations for the task: {}", migrationData.getTaskKey());
  }

  private void executeMigrationAddTransaction(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();

    omMetadataManager.getJobworkerMigrationKeysTxTable()
        .putWithBatch(batchOperation, migrationData.getTransactionKey(), migrationData.getTransaction());
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, migrationData.getTaskKey(), migrationData.getTask());

    LOG.debug("Executed migration adds transaction DB operations for the task: {}", migrationData.getTaskKey());
  }

  private void executeMigrationDeleteTransactions(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation, DBUpdateResult result) throws IOException {
    MigrationOperationData migrationData = (MigrationOperationData) result.getOperationData();
    for (String transactionKey : migrationData.getTransactionKeysToDelete()) {
      omMetadataManager.getJobworkerMigrationKeysTxTable()
          .deleteWithBatch(batchOperation, transactionKey);
    }
  }

  /**
   * Generic result class that encapsulates computed results from the request phase.
   * This class is designed to be extensible for different types of DB update operations.
   */
  public static final class DBUpdateResult {
    private final MigrationKeyOperationType operationType;
    private final Object operationData;

    private DBUpdateResult(MigrationKeyOperationType operationType, Object operationData) {
      Preconditions.checkNotNull(operationType);
      Preconditions.checkNotNull(operationData);
      this.operationType = operationType;
      this.operationData = operationData;
    }

    public static DBUpdateResult createMigrationKeyCompleteTxResult(String taskKey, String transactionKey,
        JobworkerMigrationKeysTaskProto updatedTask) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      Preconditions.checkArgument(!StringUtils.isEmpty(transactionKey));
      Preconditions.checkNotNull(updatedTask);
      MigrationOperationData migrationData = new MigrationOperationData(taskKey, transactionKey, updatedTask);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_COMPLETE_TRANSACTION, migrationData);
    }

    public static DBUpdateResult createMigrationUpdateTaskStatusResult(String taskKey,
        JobworkerMigrationKeysTaskProto updatedTask) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      Preconditions.checkNotNull(updatedTask);
      MigrationOperationData migrationData = new MigrationOperationData(taskKey, updatedTask);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_UPDATE_TASK_STATUS, migrationData);
    }

    public static DBUpdateResult createMigrationMarkScanningCompletedResult(String taskKey,
        JobworkerMigrationKeysTaskProto updatedTask) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      Preconditions.checkNotNull(updatedTask);
      MigrationOperationData migrationData = new MigrationOperationData(taskKey, updatedTask);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_MARK_SCANNING_COMPLETED, migrationData);
    }

    public static DBUpdateResult createMigrationCleanupTaskResult(String taskKey) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      MigrationOperationData migrationData = new MigrationOperationData(taskKey);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_CLEANUP_TASK, migrationData);
    }

    public static DBUpdateResult createMigrationCreateTaskResult(String taskKey,
        JobworkerMigrationKeysTaskProto createdTask) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      Preconditions.checkNotNull(createdTask);
      MigrationOperationData migrationData = new MigrationOperationData(taskKey, null, createdTask);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_CREATE_TASK, migrationData);
    }

    public static DBUpdateResult createMigrationAddTransactionResult(String taskKey, String transactionKey,
        JobworkerMigrationKeysTaskProto updatedTask, JobworkerMigrationKeysTxProto addedTransaction) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      Preconditions.checkArgument(!StringUtils.isEmpty(transactionKey));
      Preconditions.checkNotNull(updatedTask);
      Preconditions.checkNotNull(addedTransaction);
      MigrationOperationData migrationData = new MigrationOperationData(
          taskKey, transactionKey, updatedTask, addedTransaction);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_ADD_TRANSACTION, migrationData);
    }

    public static DBUpdateResult createMigrationDeleteTransactionsResult(String taskKey,
        List<String> transactionKeys) {
      Preconditions.checkArgument(!StringUtils.isEmpty(taskKey));
      Preconditions.checkNotNull(transactionKeys);
      MigrationOperationData migrationData = new MigrationOperationData(taskKey, transactionKeys);
      return new DBUpdateResult(MigrationKeyOperationType.KEY_MIGRATION_DELETE_TRANSACTIONS, migrationData);
    }

    public MigrationKeyOperationType getOperationType() {
      return operationType;
    }

    public Object getOperationData() {
      return operationData;
    }
  }

  /**
   * Specific data class for migration-related operations.
   * This encapsulates all the data needed for migration DB operations.
   */
  public static final class MigrationOperationData {
    private final String taskKey;
    private final String transactionKey;
    private final JobworkerMigrationKeysTaskProto task;
    private final JobworkerMigrationKeysTxProto transaction;
    private final List<String> transactionKeysToDelete;

    public MigrationOperationData(String taskKey) {
      this(taskKey, null, null, null, null);
    }

    public MigrationOperationData(String taskKey, JobworkerMigrationKeysTaskProto task) {
      this(taskKey, null, task, null, null);
    }

    public MigrationOperationData(String taskKey, String transactionKey, JobworkerMigrationKeysTaskProto task) {
      this(taskKey, transactionKey, task, null, null);
    }

    public MigrationOperationData(String taskKey, String transactionKey, JobworkerMigrationKeysTaskProto task,
        JobworkerMigrationKeysTxProto transaction) {
      this(taskKey, transactionKey, task, transaction, null);
    }

    public MigrationOperationData(String taskKey, List<String> transactionKeysToDelete) {
      this(taskKey, null, null, null, transactionKeysToDelete);
    }

    private MigrationOperationData(String taskKey, String transactionKey,
        JobworkerMigrationKeysTaskProto task, JobworkerMigrationKeysTxProto transaction,
        List<String> transactionKeysToDelete) {
      this.taskKey = taskKey;
      this.transactionKey = transactionKey;
      this.task = task;
      this.transaction = transaction;
      this.transactionKeysToDelete = transactionKeysToDelete;
    }

    public String getTaskKey() {
      return taskKey;
    }

    public String getTransactionKey() {
      return transactionKey;
    }

    public JobworkerMigrationKeysTaskProto getTask() {
      return task;
    }

    public JobworkerMigrationKeysTxProto getTransaction() {
      return transaction;
    }

    public List<String> getTransactionKeysToDelete() {
      return transactionKeysToDelete;
    }
  }
}
