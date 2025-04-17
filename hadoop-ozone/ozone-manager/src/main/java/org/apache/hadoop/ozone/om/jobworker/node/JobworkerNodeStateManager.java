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

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeAlreadyExistsException;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state of all the Jobworker in the cluster.
 * All the Jobworker state change should happen only via OM side.
 */
public class JobworkerNodeStateManager implements Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(JobworkerNodeStateManager.class);

  private final ReadWriteLock lock;
  /**
   * Node id to node info map.
   */
  private final ConcurrentHashMap<UUID, JobworkerInfo> nodeMap;

  public JobworkerNodeStateManager() {
    this.lock = new ReentrantReadWriteLock();
    nodeMap = new ConcurrentHashMap<>();
  }


  /**
   * Returns JobworkerInfo for the given node id.
   * @param uuid Node Id
   * @return JobworkerInfo of the node
   * @throws JobworkerNodeNotFoundException if the node is not present
   */
  public JobworkerInfo getNodeInfo(UUID uuid) throws JobworkerNodeNotFoundException {
    lock.readLock().lock();
    try {
      if (!nodeMap.containsKey(uuid)) {
        throw new JobworkerNodeNotFoundException("Jobworker UUID: " + uuid);
      }
      return nodeMap.get(uuid);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds a new jobworker node to the state manager.
   * @param jobworkerDetails jobworkerDetails
   * @throws JobworkerNodeAlreadyExistsException if the jobworker node is already present
   */
  public void addNode(JobworkerDetails jobworkerDetails) throws JobworkerNodeAlreadyExistsException {
    JobworkerNodeStatus newNodeStatus = newNodeStatus(jobworkerDetails);
    addNodeInternal(jobworkerDetails, newNodeStatus);
  }


  /**
   * @param jobworker JobworkerDetails reported by the jobworker
   */
  private JobworkerNodeStatus newNodeStatus(JobworkerDetails jobworker) {
    HddsProtos.NodeOperationalState opState = jobworker.getOperationalState();
    NodeState state = HEALTHY;

    if (opState != NodeOperationalState.IN_SERVICE) {
      LOG.info("Updating nodeOperationalState on registration as the " +
              "jobworker has a persisted state of {}", opState);
      return new JobworkerNodeStatus(opState, state);
    } else {
      return new JobworkerNodeStatus(NodeOperationalState.IN_SERVICE, state);
    }
  }


  /**
   * Adds a node to NodeStateMap.
   * @param jobworkerDetails jobworkerDetails
   * @param nodeStatus initial JobworkerNodeStatus
   * @throws JobworkerNodeAlreadyExistsException if the node already exists
   */
  private void addNodeInternal(JobworkerDetails jobworkerDetails, JobworkerNodeStatus nodeStatus)
      throws JobworkerNodeAlreadyExistsException {
    lock.writeLock().lock();
    try {
      UUID id = jobworkerDetails.getUuid();
      if (nodeMap.containsKey(id)) {
        throw new JobworkerNodeAlreadyExistsException("Node UUID: " + id);
      }
      nodeMap.put(id, new JobworkerInfo(jobworkerDetails, nodeStatus));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {

  }
}
