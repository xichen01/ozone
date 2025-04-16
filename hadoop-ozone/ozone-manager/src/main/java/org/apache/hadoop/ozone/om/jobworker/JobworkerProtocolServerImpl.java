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

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;

/**
 * Server-side implementation of the Jobworker protocol that processes client requests.
 */
public class JobworkerProtocolServerImpl implements JobworkerProtocol {
  private final OzoneManager om;

  public JobworkerProtocolServerImpl(OzoneManager om) {
    this.om = om;
  }

  @Override
  public GetOMVersionResponse getOMVersion(GetOMVersionRequest getOMVersionRequest)
      throws IOException {
    return GetOMVersionResponse.newBuilder().build();
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(
      RegisterJobworkerRequest registerJobworkerRequest) throws IOException {
    return RegisterJobworkerResponse.newBuilder().build();
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
