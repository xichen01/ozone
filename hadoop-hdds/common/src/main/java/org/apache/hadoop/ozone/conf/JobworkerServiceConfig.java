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

package org.apache.hadoop.ozone.conf;

import static org.apache.hadoop.ozone.conf.JobworkerServiceConfig.CONFIG_PREFIX;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Job worker service configuration.
 */
@ConfigGroup(prefix = CONFIG_PREFIX)
public class JobworkerServiceConfig {
  static final String CONFIG_PREFIX = "ozone.om.jobworker";

  @Config(key = "grpc.executor.thread.num",
      defaultValue = "32",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Number of threads in the gRPC executor.")
  private int grpcExecutorThreadNum;

  public int getGrpcExecutorThreadNum() {
    return grpcExecutorThreadNum;
  }

  public JobworkerServiceConfig setGrpcExecutorThreadNum(int grpcExecutorThreadNum) {
    this.grpcExecutorThreadNum = grpcExecutorThreadNum;
    return this;
  }

  @Config(key = "grpc.port",
      type = ConfigType.INT,
      defaultValue = "8982",
      tags = {ConfigTag.JOBWORKER},
      description = "Port used for the Job Worker gRPC service.")
  private int grpcPort;
  static final String GRPC_PORT_KEY = "grpc.port";

  public int getGrpcPort() {
    return grpcPort;
  }

  public static String getGrpcPortKey() {
    return CONFIG_PREFIX + "." + GRPC_PORT_KEY;
  }

  public JobworkerServiceConfig setGrpcPort(int grpcPort) {
    this.grpcPort = grpcPort;
    return this;
  }

  @Config(key = "grpc.bossgroup.size",
      defaultValue = "1",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Number of threads in the gRPC boss group.")
  private int grpcBossGroupSize;

  public int getGrpcBossGroupSize() {
    return grpcBossGroupSize;
  }

  public JobworkerServiceConfig setGrpcBossGroupSize(int grpcBossGroupSize) {
    this.grpcBossGroupSize = grpcBossGroupSize;
    return this;
  }

  @Config(key = "grpc.workergroup.size",
      defaultValue = "8",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Number of threads in the gRPC worker group.")
  private int grpcWorkerGroupSize;

  public int getGrpcWorkerGroupSize() {
    return grpcWorkerGroupSize;
  }

  public JobworkerServiceConfig setGrpcWorkerGroupSize(int grpcWorkerGroupSize) {
    this.grpcWorkerGroupSize = grpcWorkerGroupSize;
    return this;
  }

  @Config(key = "grpc.maximum.inbound.length",
      defaultValue = "32M",
      type = ConfigType.SIZE,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum inbound message size for gRPC.")
  private int grpcMaximumInboundLength;

  public int getGrpcMaximumInboundLength() {
    return grpcMaximumInboundLength;
  }

  public JobworkerServiceConfig setGrpcMaximumInboundLength(int grpcMaximumInboundLength) {
    this.grpcMaximumInboundLength = grpcMaximumInboundLength;
    return this;
  }

  @Config(key = "stalenode.interval",
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Time interval after which a jobworker is marked as stale " +
          "if it does not send heartbeats.")
  private long staleNodeIntervalMs;

  public long getStaleNodeIntervalMs() {
    return staleNodeIntervalMs;
  }

  public JobworkerServiceConfig setStaleNodeIntervalMs(long staleNodeIntervalMs) {
    this.staleNodeIntervalMs = staleNodeIntervalMs;
    return this;
  }

  @Config(key = HEARTBEAT_PROCESS_INTERVAL_KEY,
      defaultValue = "30s",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Time interval at which the OM processes jobworker heartbeats " +
          "to check for stale nodes.")
  private long heartbeatProcessIntervalMs;
  private static final String HEARTBEAT_PROCESS_INTERVAL_KEY = "heartbeat.process.interval";

  public static String getJobworkerServiceConfigKey() {
    return CONFIG_PREFIX + "." + HEARTBEAT_PROCESS_INTERVAL_KEY;
  }

  public long getHeartbeatProcessIntervalMs() {
    return heartbeatProcessIntervalMs;
  }

  public JobworkerServiceConfig setHeartbeatProcessIntervalMs(long heartbeatProcessIntervalMs) {
    this.heartbeatProcessIntervalMs = heartbeatProcessIntervalMs;
    return this;
  }

  @Config(key = "removal.timeout.ms",
      defaultValue = "1h",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Time after which a stale jobworker is removed from OM's memory.")
  private long removalTimeoutMs;

  public long getRemovalTimeoutMs() {
    return removalTimeoutMs;
  }

  public JobworkerServiceConfig setRemovalTimeoutMs(long removalTimeoutMs) {
    this.removalTimeoutMs = removalTimeoutMs;
    return this;
  }

  @Config(key = "migration.key.command.max.retry.count",
      defaultValue = "3",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum retry count for failed migration commands.")
  private int migrationKeyCommandMaxRetryCount;

  public int getMigrationKeyCommandMaxRetryCount() {
    return migrationKeyCommandMaxRetryCount;
  }

  public JobworkerServiceConfig setMigrationKeyCommandMaxRetryCount(int migrationKeyCommandMaxRetryCount) {
    this.migrationKeyCommandMaxRetryCount = migrationKeyCommandMaxRetryCount;
    return this;
  }
}
