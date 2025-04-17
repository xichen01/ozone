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
      description = "Maximum inbound message size for gRPC in bytes.")
  private int grpcMaximumInboundLength;

  public int getGrpcMaximumInboundLength() {
    return grpcMaximumInboundLength;
  }

  public JobworkerServiceConfig setGrpcMaximumInboundLength(int grpcMaximumInboundLength) {
    this.grpcMaximumInboundLength = grpcMaximumInboundLength;
    return this;
  }
}
