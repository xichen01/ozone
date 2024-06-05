/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.util.PerformanceMetrics;
import org.apache.hadoop.util.PerformanceMetricsInitializer;

import java.util.EnumMap;
import java.util.Map;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_VOLUME_METADATA_OP_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY;

/**
 * This class is used to track Volume IO stats for each HDDS Volume.
 */
public class VolumeIOStats implements MetricsSource {
  /**
   * Lists the types of file system operations.
   */
  public enum Operation {
    OPEN,
    DELETE,
  }
  private static final String SOURCE_NAME = VolumeIOStats.class.getSimpleName();
  private final String identifier;
  private String storageDirectory;
  private static final MetricsInfo OP_TAG = Interns.info(
      "Operation", "Type of file system operations");
  private static final MetricsInfo DIR_TAG = Interns.info(
      "StorageDirectory", "Storage Directory of this Volume");

  private MetricsRegistry register = new MetricsRegistry(SOURCE_NAME);
  private @Metric MutableCounterLong readBytes;
  private @Metric MutableCounterLong readOpCount;
  private @Metric MutableCounterLong writeBytes;
  private @Metric MutableCounterLong writeOpCount;
  private @Metric MutableCounterLong readTime;
  private @Metric MutableCounterLong writeTime;
  private final Map<Operation, PerformanceMetrics> metadataOpLatencyMs = new EnumMap<>(Operation.class);

  @Deprecated
  public VolumeIOStats() {
    identifier = "";
    init();
  }

  /**
   * @param identifier Typically, path to volume root. e.g. /data/hdds
   */
  public VolumeIOStats(String identifier, String storageDirectory, ConfigurationSource conf) {
    this.identifier = identifier;
    this.storageDirectory = storageDirectory;
    int[] intervals = conf.getInts(HDDS_VOLUME_METADATA_OP_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);
    for (Operation op : Operation.values()) {
      metadataOpLatencyMs.put(op, PerformanceMetricsInitializer.getMetrics(
          new MetricsRegistry(op.name()),
          "MetadataOp",
          op + " op",
          "Ops", "TimeMs", intervals));
    }
    init();
  }

  public void init() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(getMetricsSourceName(), "Volume I/O Statistics", this);
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(getMetricsSourceName());
  }

  public String getMetricsSourceName() {
    return SOURCE_NAME + '-' + identifier;
  }

  /**
   * Increment number of bytes read from the volume.
   * @param bytesRead
   */
  public void incReadBytes(long bytesRead) {
    readBytes.incr(bytesRead);
  }

  /**
   * Increment the read operations performed on the volume.
   */
  public void incReadOpCount() {
    readOpCount.incr();
  }

  /**
   * Increment number of bytes written on to the volume.
   * @param bytesWritten
   */
  public void incWriteBytes(long bytesWritten) {
    writeBytes.incr(bytesWritten);
  }

  /**
   * Increment the write operations performed on the volume.
   */
  public void incWriteOpCount() {
    writeOpCount.incr();
  }

  /**
   * Increment the time taken by read operation on the volume.
   * @param time
   */
  public void incReadTime(long time) {
    readTime.incr(time);
  }

  /**
   * Increment the time taken by write operation on the volume.
   * @param time
   */
  public void incWriteTime(long time) {
    writeTime.incr(time);
  }

  /**
   * Returns total number of bytes read from the volume.
   * @return long
   */
  public long getReadBytes() {
    return readBytes.value();
  }

  /**
   * Returns total number of bytes written to the volume.
   * @return long
   */
  public long getWriteBytes() {
    return writeBytes.value();
  }

  /**
   * Returns total number of read operations performed on the volume.
   * @return long
   */
  public long getReadOpCount() {
    return readOpCount.value();
  }

  /**
   * Returns total number of write operations performed on the volume.
   * @return long
   */
  public long getWriteOpCount() {
    return writeOpCount.value();
  }

  /**
   * Returns total read operations time on the volume.
   * @return long
   */
  public long getReadTime() {
    return readTime.value();
  }

  /**
   * Returns total write operations time on the volume.
   * @return long
   */
  public long getWriteTime() {
    return writeTime.value();
  }

  @Metric
  public String getStorageDirectory() {
    return storageDirectory;
  }

  public PerformanceMetrics getMetadataOpLatencyMs(Operation op) {
    return metadataOpLatencyMs.get(op);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
    builder.tag(DIR_TAG, storageDirectory);

    readBytes.snapshot(builder, true);
    readOpCount.snapshot(builder, true);
    writeBytes.snapshot(builder, true);
    writeOpCount.snapshot(builder, true);
    readTime.snapshot(builder, true);
    writeTime.snapshot(builder, true);
    builder.endRecord();

    for (Operation op : Operation.values()) {
      MetricsRecordBuilder opBuilder = collector.addRecord(SOURCE_NAME);
      opBuilder.tag(OP_TAG, op.name());
      opBuilder.tag(DIR_TAG, storageDirectory);
      getMetadataOpLatencyMs(op).snapshot(opBuilder, all);
      opBuilder.endRecord();
    }
  }
}
