/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.util.SampleStat.MinMax;
import org.apache.commons.lang3.StringUtils;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * A mutable metric that tracks the minimum and maximum
 * values of a dataset over time.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableMinMax extends MutableMetric {
  /**
   * Construct a minMax metric.
   * @param registry    MetricsRegistry of the metric
   * @param name        of the metric
   * @param description of the metric
   * @param valueName   of the metric (e.g. "Time", "Latency")
   */
  public MutableMinMax(MetricsRegistry registry,
      String name, String description, String valueName) {
  }

  /**
   * Add a snapshot to the metric.
   * @param value of the metric
   */
  public synchronized void add(long value) {
    setChanged();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
  }
}
