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

package org.apache.hadoop.ozone.om.request.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateIdBatchRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Tests for {@link OMAllocateIdBatchRequest}.
 */
public class TestOMAllocateIdBatchRequest {

  public static final String TEST1 = "test1";
  public static final String TEST2 = "test2";
  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;

  @BeforeEach
  public void setup(@TempDir File tempDir) throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    omMetrics = OMMetrics.create(ozoneConfiguration);

    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS, tempDir.getAbsolutePath());

    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, ozoneManager);

    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    AuditLogger auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
  }

  @AfterEach
  public void cleanup() {
    if (omMetrics != null) {
      omMetrics.unRegister();
    }
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest = createAllocateIdBatchRequest(
        TEST1, 100);
    
    OMAllocateIdBatchRequest allocateIdBatchRequest = 
        new OMAllocateIdBatchRequest(omRequest);
    
    OMRequest preExecuteRequest = allocateIdBatchRequest.preExecute(ozoneManager);
    
    assertNotNull(preExecuteRequest);
    assertEquals(omRequest.getAllocateIdBatchRequest().getSequenceIdName(),
        preExecuteRequest.getAllocateIdBatchRequest().getSequenceIdName());
    assertEquals(omRequest.getAllocateIdBatchRequest().getBatchSize(),
        preExecuteRequest.getAllocateIdBatchRequest().getBatchSize());
  }

  @Test
  public void testPreExecuteWithInvalidBatchSize() throws Exception {
    OMAllocateIdBatchRequest allocateIdBatchRequest =
        new OMAllocateIdBatchRequest(createAllocateIdBatchRequest(TEST1, 0));

    OMException exception = assertThrows(OMException.class,
        () -> allocateIdBatchRequest.preExecute(ozoneManager));
    assertTrue(exception.getMessage().contains("Invalid batch size"));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    // First allocation
    OMRequest omRequest1 = createAllocateIdBatchRequest(
        TEST1, 50);
    OMAllocateIdBatchRequest request1 = new OMAllocateIdBatchRequest(omRequest1);
    
    OMClientResponse response1 = request1.validateAndUpdateCache(ozoneManager, 1L);
    assertEquals(1L, response1.getOMResponse()
        .getAllocateIdBatchResponse().getStartId());
    assertEquals(50L, response1.getOMResponse()
        .getAllocateIdBatchResponse().getEndId());
    
    // Second allocation should continue from where the first left off
    OMRequest omRequest2 = createAllocateIdBatchRequest(
        TEST1, 30);
    OMAllocateIdBatchRequest request2 = new OMAllocateIdBatchRequest(omRequest2);
    
    OMClientResponse response2 = request2.validateAndUpdateCache(ozoneManager, 2L);
    assertEquals(51L, response2.getOMResponse()
        .getAllocateIdBatchResponse().getStartId());
    assertEquals(80L, response2.getOMResponse()
        .getAllocateIdBatchResponse().getEndId());
  }

  @Test
  public void testValidateAndUpdateCacheWithDifferentSequenceNames() throws Exception {
    // Allocate for TEST1
    OMRequest omRequest1 = createAllocateIdBatchRequest(
        TEST1, 100);
    OMAllocateIdBatchRequest request1 = new OMAllocateIdBatchRequest(omRequest1);

    OMClientResponse response1 = request1.validateAndUpdateCache(ozoneManager, 1L);
    assertEquals(1L, response1.getOMResponse()
        .getAllocateIdBatchResponse().getStartId());

    // Allocate for TEST2 - should start from 1 again
    OMRequest omRequest2 = createAllocateIdBatchRequest(
        TEST2, 200);
    OMAllocateIdBatchRequest request2 = new OMAllocateIdBatchRequest(omRequest2);

    OMClientResponse response2 = request2.validateAndUpdateCache(ozoneManager, 2L);
    assertEquals(1L, response2.getOMResponse()
        .getAllocateIdBatchResponse().getStartId());
    assertEquals(200L, response2.getOMResponse()
        .getAllocateIdBatchResponse().getEndId());
  }

  private OMRequest createAllocateIdBatchRequest(String sequenceIdName, int batchSize) {
    AllocateIdBatchRequest allocateIdBatchRequest = AllocateIdBatchRequest.newBuilder()
        .setSequenceIdName(sequenceIdName)
        .setBatchSize(batchSize)
        .build();
    
    return OMRequest.newBuilder()
        .setAllocateIdBatchRequest(allocateIdBatchRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateIdBatch)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }
}
