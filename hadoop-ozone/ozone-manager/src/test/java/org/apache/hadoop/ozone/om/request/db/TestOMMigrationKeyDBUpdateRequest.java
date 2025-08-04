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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StoragePolicyProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.migrationKey.OMMigrationKeyDBUpdateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyArgs;
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

  @BeforeEach
  public void setup(@TempDir File tempDir) throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
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
    String taskKey = "migration-task-1";
    String transactionKey = "migration-tx-1";
    int failedKeyCount = 2;
    
    // Create migration task
    JobworkerMigrationKeysTaskProto migrationTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, migrationTask);
    
    // Create migration transaction
    JobworkerMigrationKeysTxProto migrationTx = createMigrationTransaction(transactionKey, taskKey, 5);
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
    String taskKey = "migration-task-1";
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
    String taskKey = "migration-task-1";
    
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
    String taskKey = "migration-task-1";
    
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
    String taskKey = "non-existent-task";
    
    OMRequest omRequest = createUpdateTaskStatusRequest(taskKey, JobworkerTaskStatus.COMPLETED);
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
        .build();
  }

  private JobworkerMigrationKeysTxProto createMigrationTransaction(String transactionKey,
      String taskKey, int keyCount) {
    JobworkerMigrationKeysTxProto.Builder builder = JobworkerMigrationKeysTxProto.newBuilder()
        .setTxId(1)
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setStoragePolicy(StoragePolicyProto.HOT)
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
