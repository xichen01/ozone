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

package org.apache.hadoop.ozone.jobworker.protocolPB;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceGrpc;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceGrpc.JobworkerServiceBlockingStub;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerCommandType;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatResponseProto;
import org.apache.hadoop.ozone.conf.JobworkerServiceConfig;
import org.apache.hadoop.ozone.jobworker.JobworkerClientConfiguration;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side translator to translate Java method calls to gRPC calls.
 */
public class JobworkerProtocolClientSideTranslatorPB implements JobworkerProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerProtocolClientSideTranslatorPB.class);
  private final JobworkerServiceBlockingStub rpcProxy;
  private ManagedChannel channel;

  /**
   * Constructs a Client-side proxy for Jobworker gRPC Service.
   *
   * @param omHostname The OM host Address.
   * @param conf       The OzoneConfiguration
   */
  public JobworkerProtocolClientSideTranslatorPB(String omHostname, OzoneConfiguration conf) {
    this(omHostname, -1, conf);
  }

  /**
   * Constructs a Client-side proxy for Jobworker gRPC Service.
   *
   * @param omHostname The OM host Address.
   * @param conf       The OzoneConfiguration
   * @param port       The port to connect to. If set to -1, the default gRPC port
   *                   from {@link JobworkerServiceConfig#getGrpcPort()} will be used.
   */
  public JobworkerProtocolClientSideTranslatorPB(String omHostname, int port, OzoneConfiguration conf) {
    JobworkerClientConfiguration jobworkerServiceConfig = conf.getObject(JobworkerClientConfiguration.class);
    Duration timeoutDuration = jobworkerServiceConfig.getGrpcTimeout();
    int maxInboundLength = jobworkerServiceConfig.getGrpcMaximumInboundLength();
    if (port < 0) {
      JobworkerServiceConfig jwsConf = conf.getObject(JobworkerServiceConfig.class);
      port = jwsConf.getGrpcPort();
    }
    this.channel = NettyChannelBuilder
        .forAddress(omHostname, port)
        .usePlaintext()
        .maxInboundMessageSize(maxInboundLength)
        .build();
    this.rpcProxy = JobworkerServiceGrpc
        .newBlockingStub(channel).withDeadlineAfter(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Submits a Jobworker request and returns a response.
   *
   * @param type            The type of Jobworker request.
   * @param builderConsumer Consumer to set request parameters.
   * @return The response from the server.
   */
  private JobworkerResponse submitRequest(JobworkerCommandType type,
                                          Consumer<JobworkerRequest.Builder> builderConsumer) {
    JobworkerRequest.Builder builder = JobworkerRequest.newBuilder().setCmdType(type);
    builderConsumer.accept(builder);
    JobworkerRequest request = builder.build();

    return rpcProxy.submitRequest(request);
  }

  @Override
  public GetOMVersionResponse getOMVersion(GetOMVersionRequest getOMVersionRequest)
      throws IOException {
    return submitRequest(JobworkerCommandType.GET_OM_VERSION,
        builder -> builder.setGetOMVersionRequest(getOMVersionRequest))
        .getGetOMVersionResponse();
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(
      RegisterJobworkerRequest registerRequest) throws IOException {
    return submitRequest(JobworkerCommandType.REGISTER_JOBWORKER,
        builder -> builder.setRegisterJobworkerRequest(registerRequest))
        .getRegisterJobworkerResponse();
  }

  @Override
  public SendHeartbeatResponseProto sendHeartbeat(SendHeartbeatRequest heartbeatRequest)
      throws IOException {
    return submitRequest(JobworkerCommandType.SEND_HEARTBEAT,
        builder -> builder.setSendHeartbeatRequest(heartbeatRequest))
        .getSendHeartbeatResponseProto();
  }

  @Override
  public void close() throws IOException {
    if (channel == null) {
      return;
    }
    try {
      this.channel.shutdown();
      if (!this.channel.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Channel did not terminate gracefully within 30 seconds");
        this.channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      this.channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
