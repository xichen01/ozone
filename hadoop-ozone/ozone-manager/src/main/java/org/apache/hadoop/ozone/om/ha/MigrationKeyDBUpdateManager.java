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

package org.apache.hadoop.ozone.om.ha;

import java.io.IOException;
import java.util.List;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.AddTransaction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.CreateTask;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyOperationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyDBUpdateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.CompleteTransaction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.UpdateTaskStatus;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.CallId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level API for database update operations for migration key.
 */
public class MigrationKeyDBUpdateManager {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationKeyDBUpdateManager.class);

  private final OzoneManager ozoneManager;
  private final ClientId clientId;

  /**
   * Constructor for DBUpdateManager.
   *
   * @param ozoneManager The OzoneManager instance
   */
  public MigrationKeyDBUpdateManager(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
    this.clientId = ClientId.randomId();
  }

  /**
   * Complete a migration transaction.
   *
   * @param taskKey        The task key
   * @param transactionId  The transaction ID
   * @param failedKeyCount Number of failed keys
   * @throws IOException if the operation fails
   */
  public void keyMigrationCompleteTransaction(
      String taskKey, long transactionId, int failedKeyCount) throws IOException {

    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, transactionId);

    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .setCompleteTransaction(CompleteTransaction.newBuilder()
            .setTransactionKey(transactionKey)
            .setFailedKeyCount(failedKeyCount)
            .build())
        .build();

    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_COMPLETE_TRANSACTION)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();

    executeOperation(request);
    LOG.debug("Successfully completed migration transaction for task {}, txId {}, failed keys: {}",
        taskKey, transactionId, failedKeyCount);
  }

  /**
   * Update migration task status.
   *
   * @param taskKey   The task key
   * @param newStatus The new status
   * @throws IOException if the operation fails
   */
  public void keyMigrationUpdateTaskStatus(
      String taskKey, JobworkerTaskStatus newStatus) throws IOException {

    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .setUpdateTaskStatus(UpdateTaskStatus.newBuilder()
            .setNewStatus(newStatus)
            .build())
        .build();

    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_UPDATE_TASK_STATUS)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();

    executeOperation(request);
    LOG.debug("Successfully updated migration status to {} for task: {}", newStatus, taskKey);
  }

  /**
   * Mark scanning as completed for a migration task.
   *
   * @param taskKey The task key
   * @throws IOException if the operation fails
   */
  public void keyMigrationMarkScanningCompleted(String taskKey) throws IOException {

    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .build();

    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_MARK_SCANNING_COMPLETED)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();

    executeOperation(request);
    LOG.debug("Successfully marked scanning as completed for task: {}", taskKey);
  }

  /**
   * Cleanup a migration task.
   *
   * @param taskKey The task key
   * @throws IOException if the operation fails
   */
  public void keyMigrationCleanupTask(String taskKey) throws IOException {

    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .build();

    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_CLEANUP_TASK)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();

    executeOperation(request);
    LOG.debug("Successfully cleaned up migration task: {}", taskKey);
  }

  /**
   * Create a new migration task.
   *
   * @param taskKey       The task key
   * @param ecReplicationConfig target EC replication configuration
   * @throws IOException if the operation fails
   */
  public void keyMigrationCreateTask(String taskKey, String ruleId,
      ECReplicationConfig ecReplicationConfig) throws IOException {

    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .setCreateTask(CreateTask
            .newBuilder()
            .setRuleId(ruleId)
            .setEcReplicationConfig(ecReplicationConfig.toProto())
            .build())
        .build();

    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_CREATE_TASK)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();

    executeOperation(request);
    LOG.debug("Successfully created a migration task: {}", taskKey);
  }

  /**
   * Add a new migration transaction for the specific migration task.
   *
   * @param taskKey The task key
   * @throws IOException if the operation fails
   */
  public void keyMigrationAddTransaction(String taskKey,
      long txId, JobworkerMigrationKeysTxProto migrationKeysTxProto) throws IOException {

    AddTransaction addTransaction = AddTransaction.newBuilder()
        .setTxId(txId)
        .setMigrationKeysTxProto(migrationKeysTxProto)
        .build();

    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .setAddTransaction(addTransaction)
        .build();

    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_ADD_TRANSACTION)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();

    executeOperation(request);
    LOG.debug("Successfully added a migration transaction for the task: {}", taskKey);
  }

  public void cancelMigrationTask(String taskKey) throws IOException {
    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .setCancelTask(MigrationKeyArgs.CancelTask.newBuilder().build())
        .build();
    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_CANCEL_TASK)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();
    executeOperation(request);
  }

  public void deleteTransactions(String taskKey, List<String> transactionKeys)
      throws IOException {
    MigrationKeyArgs migrationKeyArgs = MigrationKeyArgs.newBuilder()
        .setTaskKey(taskKey)
        .setDeleteTransactions(MigrationKeyArgs.DeleteTransactions.newBuilder()
            .addAllTransactionKeys(transactionKeys)
            .build())
        .build();
    MigrationKeyDBUpdateRequest request = MigrationKeyDBUpdateRequest.newBuilder()
        .setType(MigrationKeyOperationType.KEY_MIGRATION_DELETE_TRANSACTIONS)
        .setMigrationKeyArgs(migrationKeyArgs)
        .build();
    executeOperation(request);
  }

  /**
   * Execute a database update operation through OM HA.
   * This method creates the OM request, submits it, and waits for completion.
   *
   * @param request The database update request to execute
   * @throws IOException if the operation fails
   */
  private void executeOperation(MigrationKeyDBUpdateRequest request) throws IOException {
    try {
      OMRequest omRequest = createOMRequest(request);
      OMResponse response = submitRequest(omRequest);
      
      if (!response.getSuccess()) {
        String errorMsg = response.hasMessage() ? response.getMessage() : "Unknown error";
        throw new IOException("Database update operation failed: " + errorMsg);
      }
    } catch (ServiceException e) {
      throw new IOException("Failed to execute database update operation", e);
    }
  }

  /**
   * Create an OM request for the database update operation.
   *
   * @param request The database update request
   * @return OMRequest to be submitted
   */
  private OMRequest createOMRequest(MigrationKeyDBUpdateRequest request) {
    return OMRequest.newBuilder()
        .setCmdType(Type.MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(request)
        .setClientId(clientId.toString())
        .build();
  }

  /**
   * Submit request through Ratis or direct OM call and wait for response.
   *
   * @param omRequest The OM request to submit
   * @return OMResponse from the operation
   * @throws ServiceException if the request submission fails
   * @throws IOException if the response is invalid
   */
  private OMResponse submitRequest(OMRequest omRequest) throws ServiceException, IOException {
    OMResponse omResponse;
    
    if (isRatisEnabled()) {
      // Ratis submission does not perform preExecute, so do it before submitting.
      omRequest = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager)
          .preExecute(ozoneManager);
      OzoneManagerRatisServer server = ozoneManager.getOmRatisServer();
      omResponse = server.submitRequest(omRequest, clientId, CallId.getAndIncrement());
    } else {
      // Submit directly to OM
      omResponse = ozoneManager.getOmServerProtocol().submitRequest(null, omRequest);
    }
    
    if (omResponse == null) {
      throw new IOException("Received null response from OM");
    }
    
    if (!omResponse.hasMigrationKeyDBUpdateResponse()) {
      throw new IOException("No DB update response received from OM");
    }
    
    return omResponse;
  }

  private boolean isRatisEnabled() {
    return ozoneManager.isRatisEnabled();
  }
}
