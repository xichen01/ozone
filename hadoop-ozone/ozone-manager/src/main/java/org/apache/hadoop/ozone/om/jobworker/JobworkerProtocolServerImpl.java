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

package org.apache.hadoop.ozone.om.jobworker;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;

/**
 * Server-side implementation of the Jobworker protocol that processes client requests.
 */
public class JobworkerProtocolServerImpl implements JobworkerProtocol {
  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.OMLOGGER);
  private final OzoneManager om;
  private final JobworkerNodeManager jobworkerNodeManager;

  public JobworkerProtocolServerImpl(OzoneManager om, JobworkerNodeManager jobworkerNodeManager) {
    this.om = om;
    this.jobworkerNodeManager = jobworkerNodeManager;
  }

  @Override
  public GetOMVersionResponse getOMVersion(GetOMVersionRequest getOMVersionRequest)
      throws IOException {
    return GetOMVersionResponse.newBuilder().build();
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(
      RegisterJobworkerRequest registerJobworkerRequest) throws IOException {
    JobworkerDetails jobworkerDetails =
        JobworkerDetails.getFromProtoBuf(registerJobworkerRequest.getExtendedJobWorkDetailsProto());
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("jobworkerDetails", jobworkerDetails.toString());
    try {
      RegisterJobworkerResponse response =
          jobworkerNodeManager.registerJobworker(jobworkerDetails);
      AUDIT.logWriteSuccess(om.buildAuditMessageForSuccess(OMAction.JW_REGISTER, auditMap));
      return response;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(om.buildAuditMessageForFailure(OMAction.JW_REGISTER, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public SendHeartbeatResponseProto sendHeartbeat(SendHeartbeatRequest sendHeartbeatRequest)
      throws IOException {
    return SendHeartbeatResponseProto.newBuilder().build();
  }

  @Override
  public void close() throws IOException {
  }
}
