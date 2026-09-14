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

import static org.apache.hadoop.ozone.conf.JobWorkerMigrationKeyConfiguration.CONFIG_PREFIX;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Jobworker task configuration.
 */
@ConfigGroup(prefix = CONFIG_PREFIX)
public class JobWorkerMigrationKeyConfiguration {
  static final String CONFIG_PREFIX = "ozone.jobworker.migration.key";

  @Config(key = "ozone.jobworker.migration.key.command.max.retry.count",
      defaultValue = "3",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum retry count for failed migration commands.")
  private int commandMaxRetryCount;

  public int getCommandMaxRetryCount() {
    return commandMaxRetryCount;
  }

  public JobWorkerMigrationKeyConfiguration setCommandMaxRetryCount(int commandMaxRetryCount) {
    this.commandMaxRetryCount = commandMaxRetryCount;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.batch.size",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "100",
      description = "Maximum number of keys to process in a single migration command."
  )
  private int batchSize = 100;

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Config(key = "ozone.jobworker.migration.key.buffer.size",
      type = ConfigType.SIZE,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "64KB",
      description = "Buffer size for copying key data during migration."
  )
  private int bufferSize = 64 * 1024;

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  @Config(key = "ozone.jobworker.migration.key.thread.pool.size",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      defaultValue = "4",
      description = "Max number of threads for key migration tasks in JobWorker."
  )
  private int threadPoolSize = 4;

  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  public void setThreadPoolSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }

  @Config(key = "ozone.jobworker.migration.key.incomplete.task.timeout",
      defaultValue = "2d",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Timeout for incomplete migration tasks. If a task has not " +
          "been updated for this duration, it will be marked as completed.")
  private long incompleteTaskTimeoutMs;

  public long getIncompleteTaskTimeoutMs() {
    return incompleteTaskTimeoutMs;
  }

  public JobWorkerMigrationKeyConfiguration setIncompleteTaskTimeoutMs(
      long incompleteTaskTimeoutMs) {
    this.incompleteTaskTimeoutMs = incompleteTaskTimeoutMs;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.completed.task.retention.time",
      defaultValue = "7d",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Time to keep completed migration tasks in the status table " +
          "before cleanup.")
  private long completedTaskRetentionTimeMs;

  public long getCompletedTaskRetentionTimeMs() {
    return completedTaskRetentionTimeMs;
  }

  public JobWorkerMigrationKeyConfiguration setCompletedTaskRetentionTimeMs(
      long completedTaskRetentionTimeMs) {
    this.completedTaskRetentionTimeMs = completedTaskRetentionTimeMs;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.max.concurrent.tasks",
      defaultValue = "2",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum number of concurrent migration tasks that can run at the same time.")
  private int maxConcurrentTasks;

  public int getMaxConcurrentTasks() {
    return maxConcurrentTasks;
  }

  public JobWorkerMigrationKeyConfiguration setMaxConcurrentTasks(int maxConcurrentTasks) {
    this.maxConcurrentTasks = maxConcurrentTasks;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.max.inflight.command.count",
      defaultValue = "5",
      type = ConfigType.INT,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum number of inflight (uncompleted) migration commands that can exist at the same time. " +
          "When this limit is reached, new command sending will be blocked until some inflight commands complete.")
  private int maxInflightCommandCount;

  public int getMaxInflightCommandCount() {
    return maxInflightCommandCount;
  }

  public JobWorkerMigrationKeyConfiguration setMaxInflightCommandCount(int maxInflightCommandCount) {
    this.maxInflightCommandCount = maxInflightCommandCount;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.max.task.waiting.time",
      defaultValue = "10m",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Maximum time to wait for a migration task to complete before considering it as timeout.")
  private long maxTaskWaitingTimeMs;

  public long getMaxTaskWaitingTimeMs() {
    return maxTaskWaitingTimeMs;
  }

  public JobWorkerMigrationKeyConfiguration setMaxTaskWaitingTimeMs(long maxTaskWaitingTimeMs) {
    this.maxTaskWaitingTimeMs = maxTaskWaitingTimeMs;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.storage.policy.satisfier.interval",
      defaultValue = "10m",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Time interval at which StoragePolicySatisfier scans " +
          "JobworkerMigrationKeysTaskTable to process migration tasks.")
  private long storagePolicySatisfierIntervalMs;

  public long getStoragePolicySatisfierIntervalMs() {
    return storagePolicySatisfierIntervalMs;
  }

  public JobWorkerMigrationKeyConfiguration setStoragePolicySatisfierIntervalMs(long storagePolicySatisfierIntervalMs) {
    this.storagePolicySatisfierIntervalMs = storagePolicySatisfierIntervalMs;
    return this;
  }

  @Config(key = "ozone.jobworker.migration.key.storage.policy.satisfier.timeout",
      defaultValue = "1h",
      type = ConfigType.TIME,
      tags = {ConfigTag.JOBWORKER},
      description = "Timeout for StoragePolicySatisfier service operations.")
  private long storagePolicySatisfierTimeoutMs;

  public long getStoragePolicySatisfierTimeoutMs() {
    return storagePolicySatisfierTimeoutMs;
  }

  public JobWorkerMigrationKeyConfiguration setStoragePolicySatisfierTimeoutMs(long storagePolicySatisfierTimeoutMs) {
    this.storagePolicySatisfierTimeoutMs = storagePolicySatisfierTimeoutMs;
    return this;
  }
}
