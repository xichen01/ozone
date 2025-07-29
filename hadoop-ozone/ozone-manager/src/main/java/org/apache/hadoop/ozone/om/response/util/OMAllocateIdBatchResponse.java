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

package org.apache.hadoop.ozone.om.response.util;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SEQUENCE_ID_TABLE;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for AllocateIdBatch operation.
 * This response persists the new lastId to the database.
 */
@CleanupTableInfo(cleanupTables = {SEQUENCE_ID_TABLE})
public class OMAllocateIdBatchResponse extends OMClientResponse {
  
  private static final Logger LOG = LoggerFactory.getLogger(OMAllocateIdBatchResponse.class);
  
  private final String sequenceIdName;
  private final long newLastId;
  
  /**
   * Constructor for successful response.
   */
  public OMAllocateIdBatchResponse(OMResponse omResponse, String sequenceIdName, long newLastId) {
    super(omResponse);
    Preconditions.checkNotNull(sequenceIdName);
    this.sequenceIdName = sequenceIdName;
    this.newLastId = newLastId;
  }
  
  /**
   * Constructor for error response.
   */
  public OMAllocateIdBatchResponse(OMResponse omResponse) {
    super(omResponse);
    this.sequenceIdName = null;
    this.newLastId = -1;
    checkStatusNotOK();
  }
  
  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    
    if (getOMResponse().getSuccess()) {
      omMetadataManager.getSequenceIdTable().putWithBatch(batchOperation,
          sequenceIdName, newLastId);
      
      LOG.debug("Added sequence ID update to batch: {} -> {}", sequenceIdName, newLastId);
    }
  }
}
