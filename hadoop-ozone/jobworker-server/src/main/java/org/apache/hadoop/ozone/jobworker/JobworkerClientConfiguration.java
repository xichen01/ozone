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

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Configuration specific to JobWorker.
 */
@ConfigGroup(prefix = "ozone.jobworker.client")
public class JobworkerClientConfiguration {

  @Config(key = "rpc.timeout",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "10s",
      description = "Timeout for RPC."
  )
  private Duration rpcTimeout;

  public Duration getRpcTimeout() {
    return rpcTimeout;
  }

  public void setRpcTimeout(Duration rpcTimeout) {
    this.rpcTimeout = rpcTimeout;
  }

  @Config(key = "grpc.maximum.inbound.length",
      defaultValue = "32M",
      type = ConfigType.SIZE,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum inbound message size for gRPC in bytes.")
  private int grpcMaximumInboundLength;

  public int getGrpcMaximumInboundLength() {
    return grpcMaximumInboundLength;
  }

  public void setGrpcMaximumInboundLength(int grpcMaximumInboundLength) {
    this.grpcMaximumInboundLength = grpcMaximumInboundLength;
  }

  @Config(key = "heartbeat.interval",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "30s",
      description = "Heartbeat interval from JobWorker to OM."
  )
  private Duration heartbeatInterval = Duration.ofSeconds(30);

  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }

  @Config(key = "heartbeat.log.warn.interval",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "10",
      description = "Number of heartbeat failures before logging a warning."
  )
  private int heartbeatLogWarnInterval = 10;

  public int getHeartbeatLogWarnInterval() {
    return heartbeatLogWarnInterval;
  }

  public void setHeartbeatLogWarnInterval(int heartbeatLogWarnInterval) {
    this.heartbeatLogWarnInterval = heartbeatLogWarnInterval;
  }

}
