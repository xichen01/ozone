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

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.ServiceException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceGrpc;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerResponse;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles gRPC requests for the Jobworker protocol and
 * dispatches them to the protocol implementation.
 */
public class JobworkerGrpcRequestHandler extends
    JobworkerServiceGrpc.JobworkerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerGrpcRequestHandler.class);

  private final JobworkerProtocol jobworkerProtocol;
  private final OzoneProtocolMessageDispatcher<JobworkerRequest,
      JobworkerResponse, ProtocolMessageEnum> dispatcher;

  public JobworkerGrpcRequestHandler(
      JobworkerProtocol jobworkerProtocol,
      ProtocolMessageMetrics<ProtocolMessageEnum> protocolMessageMetrics) {
    this.jobworkerProtocol = jobworkerProtocol;
    dispatcher = new OzoneProtocolMessageDispatcher<>("OMJobworkerProtocol",
        protocolMessageMetrics, LOG);
  }

  @Override
  public void submitRequest(JobworkerRequest request,
                            StreamObserver<JobworkerResponse> responseObserver) {
    LOG.debug("JobworkerServiceGrpc: processing request {}", request.getCmdType().name());

    try {
      JobworkerResponse response = dispatcher.processRequest(request, this::processMessage,
          request.getCmdType(), request.getTraceID());
      responseObserver.onNext(response);
    } catch (ServiceException e) {
      LOG.error("Failed to process Jobworker request", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
      return;
    }
    responseObserver.onCompleted();
  }

  public JobworkerResponse processMessage(JobworkerRequest request)
      throws ServiceException {
    try {
      JobworkerResponse.Builder responseBuilder = JobworkerResponse.newBuilder()
          .setCmdType(request.getCmdType())
          .setStatus(JobworkerResponse.Status.OK);
      LOG.debug("Received Jobworker request: {}", request);
      switch (request.getCmdType()) {
      case SEND_HEARTBEAT:
        responseBuilder.setSendHeartbeatResponseProto(
            jobworkerProtocol.sendHeartbeat(request.getSendHeartbeatRequest()));
        break;
      case REGISTER_JOBWORKER:
        responseBuilder.setRegisterJobworkerResponse(
            jobworkerProtocol.registerJobworker(request.getRegisterJobworkerRequest()));
        break;
      case GET_OM_VERSION:
        responseBuilder.setGetOMVersionResponse(
            jobworkerProtocol.getOMVersion(request.getGetOMVersionRequest()));
        break;
      default:
        LOG.warn("Received an unknown Jobworker request type: {}", request.getCmdType());
        throw new ServiceException("Unknown command type: " + request.getCmdType());
      }
      return responseBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}

