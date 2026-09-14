/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.db;

import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.request.migrationKey.OMMigrationKeyDBUpdateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs.CreateTask;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyDBUpdateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyOperationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.MigrationKeyDBUpdate;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link OMMigrationKeyDBUpdateRequest}.
 */
public class TestOMMigrationKeyDBUpdateRequest {

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private final Random random = new Random();

  @BeforeEach
  public void setup(@TempDir File tempDir) throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    omMetrics = OMMetrics.create(ozoneConfiguration);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        tempDir.getAbsolutePath());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        tempDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    AuditLogger auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class)))
        .thenReturn(true);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testValidateAndUpdateCacheCompleteMigrationTransaction() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    long txId = random.nextLong();
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
    int failedKeyCount = 2;
    
    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    // Create migration transaction
    JobworkerMigrationKeysTxProto migrationTx = createMigrationTransaction(txId, taskKey, 5);
    omMetadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, migrationTx);
    
    OMRequest omRequest = createCompleteMigrationTransactionRequest(taskKey, transactionKey, failedKeyCount);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
    
    // Verify task was updated correctly
    JobworkerMigrationKeysTaskProto updatedTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(updatedTask);
    Assertions.assertEquals(8, updatedTask.getMigratedKeyCount()); // 5 + (5 - 2)
    Assertions.assertEquals(2, updatedTask.getFailedKeyCount()); // 0 + 2
  }

  @Test
  public void testValidateAndUpdateCacheUpdateTaskStatus() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    JobworkerTaskStatus newStatus = JobworkerTaskStatus.COMPLETED;
    
    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    OMRequest omRequest = createUpdateTaskStatusRequest(taskKey, newStatus);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
    
    // Verify task status was updated
    JobworkerMigrationKeysTaskProto updatedTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(updatedTask);
    Assertions.assertEquals(newStatus, updatedTask.getMigrationStatus());
  }

  @Test
  public void testValidateAndUpdateCacheMarkScanningCompleted() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    
    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    OMRequest omRequest = createMarkScanningCompletedRequest(taskKey);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
    
    // Verify scanning completed flag was set
    JobworkerMigrationKeysTaskProto updatedTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(updatedTask);
    Assertions.assertTrue(updatedTask.getCompleteScanning());
  }

  @Test
  public void testValidateAndUpdateCacheCleanupTask() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    
    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    OMRequest omRequest = createCleanupTaskRequest(taskKey);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithNonExistentTask() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    
    OMRequest omRequest = createUpdateTaskStatusRequest(taskKey, JobworkerTaskStatus.COMPLETED);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.INVALID_REQUEST, omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheCreateTask() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    
    // Verify task doesn't exist initially
    Assertions.assertNull(omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey));
    
    ECReplicationConfig ecReplicationConfig = new ECReplicationConfig("rs-6-3-1024k");
    OMRequest omRequest = createCreateTaskRequest(taskKey, "123456", ecReplicationConfig);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
    
    // Verify task was created with correct initial values
    JobworkerMigrationKeysTaskProto createdTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(createdTask);
    Assertions.assertEquals(JobworkerTaskStatus.PENDING, createdTask.getMigrationStatus());
    Assertions.assertTrue(createdTask.getStartTime() > 0);
    Assertions.assertTrue(createdTask.getLastUpdateTime() > 0);
    Assertions.assertEquals(createdTask.getStartTime(), createdTask.getLastUpdateTime());
    Assertions.assertFalse(createdTask.getCompleteScanning());
    Assertions.assertEquals(ecReplicationConfig.toProto(), createdTask.getEcReplicationConfig());
    Assertions.assertEquals("123456", createdTask.getRuleId());
    Assertions.assertEquals(0, createdTask.getTotalKeyCount());
    Assertions.assertEquals(0, createdTask.getMigratedKeyCount());
    Assertions.assertEquals(0, createdTask.getFailedKeyCount());
  }

  @Test
  public void testValidateAndUpdateCacheCreateTaskAlreadyExists() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    
    // Create existing task
    JobworkerMigrationKeysTaskProto existingTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, existingTask);
    
    OMRequest omRequest = createCreateTaskRequest(taskKey, "123",
        new ECReplicationConfig("rs-6-3-1024k"));
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.INVALID_REQUEST, omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheAddTransaction() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    long txId = random.nextLong();
    int originalKeyCount = 10;
    int originalMigratedKeyCount = 5;
    int newAddKeyCount = 3;

    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(
        taskKey, originalKeyCount, originalMigratedKeyCount, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    // Create migration transaction
    JobworkerMigrationKeysTxProto migrationTx = createMigrationTransaction(txId, taskKey, newAddKeyCount);
    OMRequest omRequest = createAddTransactionRequest(taskKey, txId, migrationTx);
    Thread.sleep(1); // make the Time.now() in the preExecute generate a difference value for the test
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
    
    // Verify task was updated correctly
    JobworkerMigrationKeysTaskProto updatedTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(updatedTask);
    Assertions.assertEquals(originalKeyCount + newAddKeyCount, updatedTask.getTotalKeyCount());
    Assertions.assertEquals(originalMigratedKeyCount, updatedTask.getMigratedKeyCount());
    Assertions.assertEquals(0, updatedTask.getFailedKeyCount());
    Assertions.assertTrue(updatedTask.getLastUpdateTime() > migrationTask.getLastUpdateTime());
    
    // Verify transaction was added
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
    JobworkerMigrationKeysTxProto addedTransaction = 
        omMetadataManager.getJobworkerMigrationKeysTxTable().get(transactionKey);
    Assertions.assertNotNull(addedTransaction);
    Assertions.assertEquals(migrationTx.getTxId(), addedTransaction.getTxId());
    Assertions.assertEquals(3, addedTransaction.getMigrationKeysCount());
  }

  @Test
  public void testValidateAndUpdateCacheAddTransactionTaskNotExists() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    long txId = random.nextLong();
    
    JobworkerMigrationKeysTxProto migrationTx = createMigrationTransaction(txId, taskKey, 3);
    OMRequest omRequest = createAddTransactionRequest(taskKey, txId, migrationTx);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.INVALID_REQUEST, omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheAddTransactionAlreadyExists() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    long txId = random.nextLong();
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
    
    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    // Create existing transaction
    JobworkerMigrationKeysTxProto existingTx = createMigrationTransaction(txId, taskKey, 2);
    omMetadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, existingTx);
    
    // Try to add duplicate transaction
    JobworkerMigrationKeysTxProto migrationTx = createMigrationTransaction(txId, taskKey, 3);
    OMRequest omRequest = createAddTransactionRequest(taskKey, txId, migrationTx);
    omRequest = new OMMigrationKeyDBUpdateRequest(omRequest).preExecute(ozoneManager);
    
    OMMigrationKeyDBUpdateRequest omDBUpdateRequest = new OMMigrationKeyDBUpdateRequest(omRequest);
    long updateID = 1000;
    OMClientResponse omClientResponse = 
        omDBUpdateRequest.validateAndUpdateCache(ozoneManager, updateID);
    
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(Status.INVALID_REQUEST, omClientResponse.getOMResponse().getStatus());
  }

  private OMRequest createCompleteMigrationTransactionRequest(String taskKey,
      String transactionKey, int failedKeyCount) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(
            MigrationKeyDBUpdateRequest.newBuilder()
                .setType(MigrationKeyOperationType.KEY_MIGRATION_COMPLETE_TRANSACTION)
                .setMigrationKeyArgs(
                    MigrationKeyArgs.newBuilder()
                        .setTaskKey(taskKey)
                        .setCompleteTransaction(
                            MigrationKeyArgs.CompleteTransaction.newBuilder()
                                .setTransactionKey(transactionKey)
                                .setFailedKeyCount(failedKeyCount)
                                .build())
                        .build())
                .build())
        .build();
  }

  private OMRequest createUpdateTaskStatusRequest(String taskKey, JobworkerTaskStatus newStatus) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(
            MigrationKeyDBUpdateRequest.newBuilder()
                .setType(MigrationKeyOperationType.KEY_MIGRATION_UPDATE_TASK_STATUS)
                .setMigrationKeyArgs(
                    MigrationKeyArgs.newBuilder()
                        .setTaskKey(taskKey)
                        .setUpdateTaskStatus(
                            MigrationKeyArgs.UpdateTaskStatus.newBuilder()
                                .setNewStatus(newStatus)
                                .build())
                        .build())
                .build())
        .build();
  }

  private OMRequest createMarkScanningCompletedRequest(String taskKey) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(
            MigrationKeyDBUpdateRequest.newBuilder()
                .setType(MigrationKeyOperationType.KEY_MIGRATION_MARK_SCANNING_COMPLETED)
                .setMigrationKeyArgs(
                    MigrationKeyArgs.newBuilder()
                        .setTaskKey(taskKey)
                        .build())
                .build())
        .build();
  }

  private OMRequest createCleanupTaskRequest(String taskKey) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(
            MigrationKeyDBUpdateRequest.newBuilder()
                .setType(MigrationKeyOperationType.KEY_MIGRATION_CLEANUP_TASK)
                .setMigrationKeyArgs(
                    MigrationKeyArgs.newBuilder()
                        .setTaskKey(taskKey)
                        .build())
                .build())
        .build();
  }

  private OMRequest createCreateTaskRequest(String taskKey, String ruleId,
      ECReplicationConfig ecReplicationConfig) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(
            MigrationKeyDBUpdateRequest.newBuilder()
                .setType(MigrationKeyOperationType.KEY_MIGRATION_CREATE_TASK)
                .setMigrationKeyArgs(
                    MigrationKeyArgs.newBuilder()
                        .setTaskKey(taskKey)
                        .setCreateTask(
                            CreateTask.newBuilder()
                                .setRuleId(ruleId)
                                .setEcReplicationConfig(ecReplicationConfig.toProto())
                                .build())
                        .build())
                .build())
        .build();
  }

  private OMRequest createAddTransactionRequest(String taskKey, long txId, 
      JobworkerMigrationKeysTxProto migrationTx) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(MigrationKeyDBUpdate)
        .setMigrationKeyDBUpdateRequest(
            MigrationKeyDBUpdateRequest.newBuilder()
                .setType(MigrationKeyOperationType.KEY_MIGRATION_ADD_TRANSACTION)
                .setMigrationKeyArgs(
                    MigrationKeyArgs.newBuilder()
                        .setTaskKey(taskKey)
                        .setAddTransaction(
                            MigrationKeyArgs.AddTransaction.newBuilder()
                                .setTxId(txId)
                                .setMigrationKeysTxProto(migrationTx)
                                .build())
                        .build())
                .build())
        .build();
  }

  private JobworkerMigrationKeysTaskProto createMigrationTask(String taskKey,
      int totalKeys, int migratedKeys, int failedKeys) {
    return JobworkerMigrationKeysTaskProto.newBuilder()
        .setMigrationStatus(JobworkerTaskStatus.EXECUTING)
        .setTotalKeyCount(totalKeys)
        .setMigratedKeyCount(migratedKeys)
        .setFailedKeyCount(failedKeys)
        .setStartTime(System.currentTimeMillis())
        .setLastUpdateTime(System.currentTimeMillis())
        .setCompleteScanning(false)
        .setRuleId(RandomStringUtils.randomAlphanumeric(32))
        .setEcReplicationConfig(new ECReplicationConfig("rs-6-3-1024k").toProto())
        .build();
  }

  private JobworkerMigrationKeysTxProto createMigrationTransaction(long txId,
      String taskKey, int keyCount) {
    JobworkerMigrationKeysTxProto.Builder builder = JobworkerMigrationKeysTxProto.newBuilder()
        .setTxId(txId)
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setEcReplicationConfig(new ECReplicationConfig("rs-6-3-1024k").toProto())
        .setTaskKey(taskKey);
    
    for (int i = 0; i < keyCount; i++) {
      builder.addMigrationKeys(
          MigrationKeyProto.newBuilder()
              .setKey("key-" + i)
              .setUpdateID(i)
              .build());
    }
    return builder.build();
  }
}
