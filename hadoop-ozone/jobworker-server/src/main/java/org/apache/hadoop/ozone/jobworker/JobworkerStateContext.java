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

package org.apache.hadoop.ozone.jobworker;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context of the Jobworker State Machine.
 */
public class JobworkerStateContext {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerStateContext.class);

  private final JobworkerClientConfiguration jwConf;
  private final Set<InetSocketAddress> endpoints;
  // Endpoint -> Boolean of whether the full report should be queued in getFullReports call.
  private final Map<InetSocketAddress, AtomicBoolean> isReportReadyToBeSent;
  private final long initializeHeartbeatFrequencyMs = 2000;
  private final AtomicLong heartbeatFrequencyMs = new AtomicLong(initializeHeartbeatFrequencyMs);
  private final String threadNamePrefix;
  private JobworkerStates state;
  private final JobworkerDetails jobworkerDetails;


  /**
   * Constructs a StateContext for JobworkerStateMachine.
   *
   * @param conf             Configuration
   * @param state            Initial state
   * @param jobworkerDetails  Details of this jobworker
   * @param threadNamePrefix Thread name prefix
   */
  public JobworkerStateContext(ConfigurationSource conf,
                               JobworkerStates state,
                               JobworkerDetails jobworkerDetails,
                               String threadNamePrefix) {
    this.jwConf = conf.getObject(JobworkerClientConfiguration.class);
    this.state = state;
    endpoints = new HashSet<>();
    isReportReadyToBeSent = new HashMap<>();
    this.threadNamePrefix = threadNamePrefix;
    this.jobworkerDetails = jobworkerDetails;
    // TODO jobworker support NodeReportProto
  }

  public JobworkerDetails getJobworkerDetails() {
    return jobworkerDetails;
  }

  /**
   * Returns the current state the machine is in.
   *
   * @return state.
   */
  public JobworkerStates getState() {
    return state;
  }

  /**
   * Sets the current state of the machine.
   *
   * @param state state.
   */
  public void setState(JobworkerStates state) {
    if (this.state != state) {
      if (this.state.isTransitionAllowedTo(state)) {
        this.state = state;
      } else {
        LOG.warn("Ignore disallowed transition from {} to {}",
            this.state, state);
      }
    }
  }

  /**
   * Configure heartbeat frequency from configuration.
   */
  public void configureHeartbeatFrequency() {
    heartbeatFrequencyMs.set(jwConf.getHeartbeatInterval().toMillis());
  }

  /**
   * Return current heartbeat frequency in ms.
   */
  public long getHeartbeatFrequencyMs() {
    return heartbeatFrequencyMs.get();
  }

  /**
   * Set heartbeat frequency manually.
   *
   * @param frequencyMs Frequency in milliseconds
   */
  public void configureHeartbeatHeartbeatFrequencyMs(long frequencyMs) {
    this.heartbeatFrequencyMs.set(frequencyMs);
  }

  /**
   * Return heartbeat intervals during initialization.
   */
  public long getInitializeHeartbeatFrequencyMs() {
    return initializeHeartbeatFrequencyMs;
  }

  /**
   * Get the thread name prefix.
   *
   * @return thread name prefix
   */
  public String getThreadNamePrefix() {
    return threadNamePrefix;
  }
}
