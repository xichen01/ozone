/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.util.PerformanceMetrics;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PERFORMANCE_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY;

/**
 * Including OM performance related metrics.
 */
public class OMPerformanceMetrics implements MetricsSource {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();
  private static MetricsRegistry registry;

  public static OMPerformanceMetrics register(OzoneConfiguration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    registry = new MetricsRegistry(SOURCE_NAME);
    int[] intervals = conf.getInts(
        OZONE_OM_PERFORMANCE_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);
    OMPerformanceMetrics omPerformanceMetrics = new OMPerformanceMetrics();
    PerformanceMetrics.initializeMetrics(
        omPerformanceMetrics, registry, "Ops", "Time", intervals);
    return ms.register(SOURCE_NAME,
        "OzoneManager Request Performance", omPerformanceMetrics);
  }


  public static void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric(about = "Overall lookupKey in nanoseconds")
  private PerformanceMetrics lookupLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private PerformanceMetrics lookupReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private PerformanceMetrics lookupGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private PerformanceMetrics lookupRefreshLocationLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private PerformanceMetrics lookupAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private PerformanceMetrics lookupResolveBucketLatencyNs;


  @Metric(about = "Overall getKeyInfo in nanoseconds")
  private PerformanceMetrics getKeyInfoLatencyNs;

  @Metric(about = "Read key info from db in getKeyInfo")
  private PerformanceMetrics getKeyInfoReadKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in getKeyInfo")
  private PerformanceMetrics getKeyInfoGenerateBlockTokenLatencyNs;

  @Metric(about = "Refresh location latency in getKeyInfo")
  private PerformanceMetrics getKeyInfoRefreshLocationLatencyNs;

  @Metric(about = "ACLs check in getKeyInfo")
  private PerformanceMetrics getKeyInfoAclCheckLatencyNs;

  @Metric(about = "Sort datanodes latency in getKeyInfo")
  private PerformanceMetrics getKeyInfoSortDatanodesLatencyNs;

  @Metric(about = "resolveBucketLink latency in getKeyInfo")
  private PerformanceMetrics getKeyInfoResolveBucketLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private PerformanceMetrics s3VolumeContextLatencyNs;

  @Metric(about = "Client requests forcing container info cache refresh")
  private PerformanceMetrics forceContainerCacheRefresh;

  @Metric(about = "checkAccess latency in nanoseconds")
  private PerformanceMetrics checkAccessLatencyNs;

  @Metric(about = "listKeys latency in nanoseconds")
  private PerformanceMetrics listKeysLatencyNs;

  @Metric(about = "Validate request latency in nano seconds")
  private PerformanceMetrics validateRequestLatencyNs;

  @Metric(about = "Validate response latency in nano seconds")
  private PerformanceMetrics validateResponseLatencyNs;

  @Metric(about = "PreExecute latency in nano seconds")
  private PerformanceMetrics preExecuteLatencyNs;

  @Metric(about = "Ratis latency in nano seconds")
  private PerformanceMetrics submitToRatisLatencyNs;

  @Metric(about = "Convert om request to ratis request nano seconds")
  private PerformanceMetrics createRatisRequestLatencyNs;

  @Metric(about = "Convert ratis response to om response nano seconds")
  private PerformanceMetrics createOmResponseLatencyNs;

  @Metric(about = "Ratis local command execution latency in nano seconds")
  private PerformanceMetrics validateAndUpdateCacheLatencyNs;

  @Metric(about = "ACLs check latency in listKeys")
  private PerformanceMetrics listKeysAclCheckLatencyNs;

  @Metric(about = "resolveBucketLink latency in listKeys")
  private PerformanceMetrics listKeysResolveBucketLatencyNs;

  public void addLookupLatency(long latencyInNs) {
    lookupLatencyNs.add(latencyInNs);
  }

  PerformanceMetrics getLookupRefreshLocationLatencyNs() {
    return lookupRefreshLocationLatencyNs;
  }


  PerformanceMetrics getLookupGenerateBlockTokenLatencyNs() {
    return lookupGenerateBlockTokenLatencyNs;
  }

  PerformanceMetrics getLookupReadKeyInfoLatencyNs() {
    return lookupReadKeyInfoLatencyNs;
  }

  PerformanceMetrics getLookupAclCheckLatencyNs() {
    return lookupAclCheckLatencyNs;
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  PerformanceMetrics getLookupResolveBucketLatencyNs() {
    return lookupResolveBucketLatencyNs;
  }

  public void addGetKeyInfoLatencyNs(long value) {
    getKeyInfoLatencyNs.add(value);
  }

  PerformanceMetrics getGetKeyInfoAclCheckLatencyNs() {
    return getKeyInfoAclCheckLatencyNs;
  }

  PerformanceMetrics getGetKeyInfoGenerateBlockTokenLatencyNs() {
    return getKeyInfoGenerateBlockTokenLatencyNs;
  }

  PerformanceMetrics getGetKeyInfoReadKeyInfoLatencyNs() {
    return getKeyInfoReadKeyInfoLatencyNs;
  }

  PerformanceMetrics getGetKeyInfoRefreshLocationLatencyNs() {
    return getKeyInfoRefreshLocationLatencyNs;
  }

  PerformanceMetrics getGetKeyInfoResolveBucketLatencyNs() {
    return getKeyInfoResolveBucketLatencyNs;
  }

  PerformanceMetrics getGetKeyInfoSortDatanodesLatencyNs() {
    return getKeyInfoSortDatanodesLatencyNs;
  }

  public void setForceContainerCacheRefresh(boolean value) {
    forceContainerCacheRefresh.add(value ? 1L : 0L);
  }

  public void setCheckAccessLatencyNs(long latencyInNs) {
    checkAccessLatencyNs.add(latencyInNs);
  }

  public void addListKeysLatencyNs(long latencyInNs) {
    listKeysLatencyNs.add(latencyInNs);
  }

  public PerformanceMetrics getValidateRequestLatencyNs() {
    return validateRequestLatencyNs;
  }

  public PerformanceMetrics getValidateResponseLatencyNs() {
    return validateResponseLatencyNs;
  }

  public PerformanceMetrics getPreExecuteLatencyNs() {
    return preExecuteLatencyNs;
  }

  public PerformanceMetrics getSubmitToRatisLatencyNs() {
    return submitToRatisLatencyNs;
  }

  public PerformanceMetrics getCreateRatisRequestLatencyNs() {
    return createRatisRequestLatencyNs;
  }

  public PerformanceMetrics getCreateOmResponseLatencyNs() {
    return createOmResponseLatencyNs;
  }

  public PerformanceMetrics getValidateAndUpdateCacheLatencyNs() {
    return validateAndUpdateCacheLatencyNs;
  }

  PerformanceMetrics getListKeysAclCheckLatencyNs() {
    return listKeysAclCheckLatencyNs;
  }

  PerformanceMetrics getListKeysResolveBucketLatencyNs() {
    return listKeysResolveBucketLatencyNs;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);
    lookupLatencyNs.snapshot(recordBuilder, all);
    lookupReadKeyInfoLatencyNs.snapshot(recordBuilder, all);
    lookupGenerateBlockTokenLatencyNs.snapshot(recordBuilder, all);
    lookupRefreshLocationLatencyNs.snapshot(recordBuilder, all);
    lookupAclCheckLatencyNs.snapshot(recordBuilder, all);
    lookupResolveBucketLatencyNs.snapshot(recordBuilder, all);
    getKeyInfoLatencyNs.snapshot(recordBuilder, all);
    getKeyInfoReadKeyInfoLatencyNs.snapshot(recordBuilder, all);
    getKeyInfoGenerateBlockTokenLatencyNs.snapshot(recordBuilder, all);
    getKeyInfoRefreshLocationLatencyNs.snapshot(recordBuilder, all);
    getKeyInfoAclCheckLatencyNs.snapshot(recordBuilder, all);
    getKeyInfoResolveBucketLatencyNs.snapshot(recordBuilder, all);
    s3VolumeContextLatencyNs.snapshot(recordBuilder, all);
    forceContainerCacheRefresh.snapshot(recordBuilder, all);
    checkAccessLatencyNs.snapshot(recordBuilder, all);
    listKeysLatencyNs.snapshot(recordBuilder, all);
    validateRequestLatencyNs.snapshot(recordBuilder, all);
    validateResponseLatencyNs.snapshot(recordBuilder, all);
    preExecuteLatencyNs.snapshot(recordBuilder, all);
    submitToRatisLatencyNs.snapshot(recordBuilder, all);
    createRatisRequestLatencyNs.snapshot(recordBuilder, all);
    createOmResponseLatencyNs.snapshot(recordBuilder, all);
    validateAndUpdateCacheLatencyNs.snapshot(recordBuilder, all);
    listKeysAclCheckLatencyNs.snapshot(recordBuilder, all);
    listKeysResolveBucketLatencyNs.snapshot(recordBuilder, all);
  }
}
