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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.apache.hadoop.ozone.OzoneConsts.CLUSTER_ID;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USE_JOBWORKER_HOSTNAME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USE_JOBWORKER_HOSTNAME_KEY;
import static org.apache.hadoop.ozone.om.OMStorage.OM_ID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse.ReturnCode;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.ozone.jobworker.command.OMJobworkerCommand;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerNodeProtocol;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.jobworker.OMJobworkerCommandQueue;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeAlreadyExistsException;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.util.ProtobufUtils;
import org.apache.hadoop.ozone.util.RemoteAddressInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains information about the jobworker on OM side.
 */
public class JobworkerNodeManager implements JobworkerNodeProtocol, Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(JobworkerNodeManager.class);

  private final JobworkerNodeStateManager nodeStateManager;
  private final boolean useHostname;
  private final Function<String, String> nodeResolver;
  private final NetworkTopology clusterMap;
  private final OMStorage omStorage;
  private final OMNodeDetails omNodeDetails;
  private final OMJobworkerCommandQueue commandQueue;
  private final OMLayoutFeature omLayoutFeature;

  public JobworkerNodeManager(Function<String, String> nodeResolver, NetworkTopology clusterMap,
                              OMStorage omStorage, OMNodeDetails omNodeDetails, OzoneConfiguration conf) {
    this.nodeStateManager = new JobworkerNodeStateManager();
    this.useHostname = conf.getBoolean(OZONE_OM_USE_JOBWORKER_HOSTNAME_KEY,
        OZONE_OM_USE_JOBWORKER_HOSTNAME_DEFAULT);
    this.nodeResolver = nodeResolver;
    this.clusterMap = clusterMap;
    this.omStorage = omStorage;
    this.omNodeDetails = omNodeDetails;
    this.commandQueue = new OMJobworkerCommandQueue();
    this.omLayoutFeature = OMLayoutFeature.getLatestVersion();
  }

  @Override
  public GetOMVersionResponse getVersion(
      GetOMVersionRequest versionRequest) {
    return GetOMVersionResponse.newBuilder()
        .setSoftwareVersion(omLayoutFeature.layoutVersion())
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OM_ID)
            .setValue(omStorage.getOmId()))
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(CLUSTER_ID)
            .setValue(omStorage.getClusterID()))
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OM_SERVICE_ID)
            .setValue(omNodeDetails.getServiceId()))
        .build();
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(JobworkerDetails jobworkerDetails) throws IOException {
    InetAddress jobworkerAddress = RemoteAddressInterceptor.REMOTE_ADDR.get();
    jobworkerDetails.setNetworkName(jobworkerDetails.getUuidString());
    final UUID uuid = jobworkerDetails.getUuid();
    if (jobworkerAddress != null) {
      final String ipAddress = jobworkerAddress.getHostAddress();
      final String hostName = jobworkerAddress.getHostName();
      jobworkerDetails.setIpAddress(ipAddress);
      jobworkerDetails.setHostName(hostName);
      String networkLocation = nodeResolver.apply(
          useHostname ? hostName : ipAddress);
      if (networkLocation != null) {
        jobworkerDetails.setNetworkLocation(networkLocation);
      }
    }

    if (!isJobworkerNodeRegistered(uuid)) {
      try {
        clusterMap.add(jobworkerDetails);
        nodeStateManager.addNode(jobworkerDetails);
        JobworkerInfo nodeInfo = nodeStateManager.getNodeInfo(uuid);
        Preconditions.checkState(nodeInfo.getParent() != null);
        LOG.info("Registered jobworker: {}", jobworkerDetails.toDebugString());
      } catch (JobworkerNodeAlreadyExistsException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Jobworker is already registered: {}",
              jobworkerDetails);
        }
      } catch (JobworkerNodeNotFoundException e) {
        LOG.error("Cannot find jobworker {} from nodeStateManager",
            jobworkerDetails);
      }
    } else {
      // Jobworker is a stateless service, so after the jobworker restart, its UUID will change,
      // So jobworker should not re-register to OM
      LOG.warn("Jobworker is already registered {}", jobworkerDetails.toDebugString());
    }

    return getRegisterJobworkerResponse(jobworkerDetails, omStorage, omNodeDetails, ReturnCode.SUCCESS);
  }

  @Override
  public Boolean isJobworkerNodeRegistered(UUID jobworkerID) {
    try {
      nodeStateManager.getNodeInfo(jobworkerID);
      return true;
    } catch (JobworkerNodeNotFoundException e) {
      return false;
    }
  }

  @Override
  public void processHeartbeat(JobworkerDetails jobworkerDetails) {
    try {
      nodeStateManager.updateLastHeartbeatTime(jobworkerDetails);
    } catch (JobworkerNodeNotFoundException e) {
      LOG.error("OM trying to process heartbeat from an " +
          "unregistered node {}. Ignoring the heartbeat.", jobworkerDetails);
    }
  }

  @Override
  public List<OMJobworkerCommand> pollJobworkerCommand(UUID jobworkerId) {
    return commandQueue.pollCommand(jobworkerId);
  }

  @Override
  public void close() throws IOException {
    if (nodeStateManager != null) {
      nodeStateManager.close();
    }
  }

  public void processNodeReport(JobworkerDetails jobworkerDetails,
                                JobworkerNodeReportProto nodeReport) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing node report from [datanode={}]",
          jobworkerDetails.getHostName());
    }
    try {
      JobworkerInfo datanodeInfo = nodeStateManager.getNodeInfo(jobworkerDetails.getUuid());
      if (nodeReport != null) {
        datanodeInfo.updateStorageReports(nodeReport.getStorageReportList());
      }
    } catch (JobworkerNodeNotFoundException e) {
      LOG.warn("Got node report from unregistered datanode {}", jobworkerDetails);
    }
  }

  private RegisterJobworkerResponse getRegisterJobworkerResponse(
      JobworkerDetails jobworkerDetails, OMStorage storage, OMNodeDetails omNode, ReturnCode returnCode) {
    RegisterJobworkerResponse.Builder builder = RegisterJobworkerResponse.newBuilder()
        .setReturnCode(returnCode)
        .setJobworkerUUID(ProtobufUtils.toProtobuf(jobworkerDetails.getUuid()))
        .setClusterID(storage.getClusterID())
        .setOmServiceId(omNode.getServiceId());
    if (!Strings.isNullOrEmpty(jobworkerDetails.getIpAddress())) {
      builder.setIpAddress(jobworkerDetails.getIpAddress());
    }
    if (!Strings.isNullOrEmpty(jobworkerDetails.getHostName())) {
      builder.setHostname(jobworkerDetails.getHostName());
    }
    if (!Strings.isNullOrEmpty(jobworkerDetails.getNetworkName())) {
      builder.setNetworkName(jobworkerDetails.getNetworkName());
    }
    if (!Strings.isNullOrEmpty(jobworkerDetails.getNetworkLocation())) {
      builder.setNetworkLocation(jobworkerDetails.getNetworkLocation());
    }
    return builder.build();
  }

  /**
   * Add a {@link OMJobworkerCommand} to the command queue, which are
   * handled by HB thread asynchronously.
   * @param uuid jobwoker uuid
   * @param command The command need to add to the command queue
   */
  public void addOMJobworkerCommand(UUID uuid, OMJobworkerCommand<? extends Message> command) {
    this.commandQueue.addCommand(uuid, command);
  }

  @VisibleForTesting
  public JobworkerNodeStateManager getNodeStateManager() {
    return nodeStateManager;
  }

}
