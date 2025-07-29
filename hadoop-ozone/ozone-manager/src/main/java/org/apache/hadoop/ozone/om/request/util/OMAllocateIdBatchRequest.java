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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.util.OMAllocateIdBatchResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateIdBatchRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateIdBatchResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.audit.OMAction.ALLOCATE_ID_BATCH;

/**
 * Handle AllocateIdBatch request for OM sequence ID generation.
 */
public class OMAllocateIdBatchRequest extends OMClientRequest {
  
  private static final Logger LOG = LoggerFactory.getLogger(OMAllocateIdBatchRequest.class);
  
  public OMAllocateIdBatchRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);
    
    AllocateIdBatchRequest allocateIdBatchRequest = omRequest.getAllocateIdBatchRequest();
    String sequenceIdName = allocateIdBatchRequest.getSequenceIdName();
    long batchSize = allocateIdBatchRequest.getBatchSize();

    if (sequenceIdName.isEmpty()) {
      LOG.info("Invalid sequence ID name, allocateIdBatchRequest sequenceIdName is empty");
      throw new OMException("Invalid sequence ID name, allocateIdBatchRequest sequenceIdName is empty",
          INVALID_REQUEST);
    }

    if (batchSize <= 0) {
      LOG.error("Invalid batch size: {}. Batch size must be greater than 0", batchSize);
      throw new OMException("Invalid batch size: " + batchSize + 
          ". Batch size must be greater than 0", INVALID_REQUEST);
    }

    return omRequest;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    
    AllocateIdBatchRequest allocateIdBatchRequest = getOmRequest().getAllocateIdBatchRequest();
    String sequenceIdName = allocateIdBatchRequest.getSequenceIdName();
    String clientId = getOmRequest().getClientId();
    long batchSize = allocateIdBatchRequest.getBatchSize();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMClientResponse omClientResponse = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;

    Map<String, String> auditMap = new LinkedHashMap<>();
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    auditMap.put("sequenceIdName", sequenceIdName);
    auditMap.put("batchSize", String.valueOf(batchSize));
    auditMap.put("clientId", clientId);
    
    try {
      // Atomically allocate the batch by reading current value and incrementing
      Long currentLastId = omMetadataManager.getSequenceIdTable().get(sequenceIdName);
      if (currentLastId == null) {
        currentLastId = 0L;
      }
      
      long startId = currentLastId + 1;
      
      // Check for overflow before calculating endId
      if (startId > Long.MAX_VALUE - batchSize + 1) {
        LOG.error("Sequence ID overflow detected for {}: currentLastId={}, batchSize={}", 
            sequenceIdName, currentLastId, batchSize);
        throw new OMException("Sequence ID overflow detected for " + sequenceIdName + 
            ": currentLastId=" + currentLastId + ", batchSize=" + batchSize,
            OMException.ResultCodes.INVALID_REQUEST);
      }
      
      long endId = startId + batchSize - 1;
      long newLastId = endId;
      
      omMetadataManager.getSequenceIdTable().addCacheEntry(
          new CacheKey<>(sequenceIdName),
          CacheValue.get(transactionLogIndex, newLastId));
      
      AllocateIdBatchResponse allocateIdBatchResponse =
          AllocateIdBatchResponse.newBuilder()
              .setStartId(startId)
              .setEndId(endId)
              .build();
      
      omResponse.setAllocateIdBatchResponse(allocateIdBatchResponse);
      omClientResponse = new OMAllocateIdBatchResponse(omResponse.build(), 
          sequenceIdName, newLastId);
      
      auditMap.put("startId", String.valueOf(startId));
      auditMap.put("endId", String.valueOf(endId));
      auditMap.put("newLastId", String.valueOf(newLastId));
      LOG.debug("Allocated ID batch for {} (clientId {}): [{}, {}], new lastId: {}",
          sequenceIdName, clientId, startId, endId, newLastId);
    } catch (IOException ex) {
      exception = ex;
      LOG.error("Failed to allocate ID batch for sequence: {}", sequenceIdName, ex);
      omClientResponse = new OMAllocateIdBatchResponse(
          createErrorOMResponse(omResponse, ex));
    }
    auditLog(auditLogger, buildAuditMessage(ALLOCATE_ID_BATCH, auditMap, exception, userInfo));
    return omClientResponse;
  }
}
