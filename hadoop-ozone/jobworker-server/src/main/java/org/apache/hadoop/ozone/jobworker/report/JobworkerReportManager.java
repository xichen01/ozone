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

package org.apache.hadoop.ozone.jobworker.report;

import com.google.protobuf.Descriptors;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.jobworker.JobworkerClientConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerReportManager is responsible for managing all the {@link JobworkerReportPublisher}
 * and also provides {@link ScheduledExecutorService} to JobworkerReportPublisher
 * which should be used for scheduling the reports. It also manages the report queue for
 * sending reports to OzoneManager endpoints.
 */
public final class JobworkerReportManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerReportManager.class);

  private final JobworkerStateContext context;
  private final List<JobworkerReportPublisher> publishers;
  private final ScheduledExecutorService executorService;
  // Store all reports to be sent to endpoints
  private final Map<InetSocketAddress, List<Message>> reportQueue;
  private final int maxReportCount;

  /**
   * Construction of {@link JobworkerReportManager} should be done via
   * {@link JobworkerReportManager.Builder}.
   *
   * @param context    StateContext, which holds the report
   * @param publishers List of publishers which generates a report
   * @param threadNamePrefix Thread name prefix
   * @param conf Ozone Config
   */
  private JobworkerReportManager(JobworkerStateContext context, List<JobworkerReportPublisher> publishers,
                                 String threadNamePrefix, ConfigurationSource conf) {
    this.context = context;
    this.publishers = publishers;
    this.executorService = HadoopExecutors.newScheduledThreadPool(
        publishers.size(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat(threadNamePrefix +
                "JobworkerReportManager-%d").build());
    this.reportQueue = Collections.synchronizedMap(new HashMap<>());
    JobworkerClientConfiguration jwConf = conf.getObject(JobworkerClientConfiguration.class);
    maxReportCount = jwConf.getMaxReportCount();
  }

  /**
   * Initializes JobworkerReportManager, also initializes all the configured
   * report publishers.
   */
  public void init() {
    for (JobworkerReportPublisher publisher : publishers) {
      publisher.init(context, executorService, this);
    }
  }

  /**
   * Shutdown the JobworkerReportManager.
   */
  public void shutdown() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to shutdown Jobworker Report Manager", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Registers an endpoint with the report manager for report queuing.
   *
   * @param endpoint The endpoint to register
   */
  public void registerEndpoint(InetSocketAddress endpoint) {
    synchronized (reportQueue) {
      if (!reportQueue.containsKey(endpoint)) {
        reportQueue.put(endpoint, new LinkedList<>());
        LOG.debug("Registered endpoint {} for report queuing", endpoint);
      }
    }
  }

  /**
   * Adds a report to the queue for all registered endpoints.
   *
   * @param report report to be added
   */
  public void addReport(Message report) {
    if (report == null) {
      return;
    }

    final Descriptors.Descriptor descriptor = report.getDescriptorForType();
    if (descriptor == null) {
      LOG.warn("Invalid report with null descriptor");
      return;
    }

    final String reportType = descriptor.getFullName();
    if (reportType == null) {
      LOG.warn("Invalid report with the null report type");
      return;
    }

    // Queue the report for each endpoint
    synchronized (reportQueue) {
      for (List<Message> queue : reportQueue.values()) {
        queue.add(report);
      }
      LOG.debug("Added report of type {} to queue for {} endpoints",
          reportType, reportQueue.size());
    }
  }

  /**
   * Returns a limited number of available reports for a specific endpoint.
   *
   * @param endpoint the endpoint
   * @return A limited number of lists of reports
   */
  public List<Message> getLimitedCountAvailableReports(InetSocketAddress endpoint) {
    return getAllAvailableReportsUpToLimit(endpoint, maxReportCount);
  }

  /**
   * Gets all available reports for a specific endpoint up to a specified limit.
   *
   * @param endpoint the endpoint
   * @param limit    maximum number of reports to return
   * @return List of reports
   */
  public List<Message> getAllAvailableReportsUpToLimit(
      InetSocketAddress endpoint, int limit) {
    List<Message> reportsToReturn = new ArrayList<>();

    synchronized (reportQueue) {
      List<Message> reportsForEndpoint = reportQueue.get(endpoint);
      if (reportsForEndpoint != null) {
        int numReportsToGet = Math.min(reportsForEndpoint.size(), limit);
        if (numReportsToGet > 0) {
          List<Message> tempList = reportsForEndpoint.subList(0, numReportsToGet);
          reportsToReturn.addAll(tempList);
          tempList.clear();
          LOG.debug("Retrieved {} reports for endpoint {}", reportsToReturn.size(), endpoint);
        }
      }
    }

    return reportsToReturn;
  }

  /**
   * Returns new {@link JobworkerReportManager.Builder} which can be used to construct.
   * {@link JobworkerReportManager}
   * @param conf  - Conf
   * @return builder - Builder.
   */
  public static Builder newBuilder(ConfigurationSource conf) {
    return new Builder(conf);
  }

  /**
   * Builder to construct {@link JobworkerReportManager}.
   */
  public static final class Builder {

    private JobworkerStateContext stateContext;
    private List<JobworkerReportPublisher> reportPublishers;
    private JobworkerReportPublisherFactory publisherFactory;
    private String threadNamePrefix = "";
    private ConfigurationSource conf;

    private Builder(ConfigurationSource conf) {
      this.reportPublishers = new ArrayList<>();
      this.publisherFactory = new JobworkerReportPublisherFactory(conf);
      this.conf = conf;
    }

    /**
     * Sets the {@link JobworkerStateContext}.
     *
     * @param context JobworkerStateContext
     *
     * @return JobworkerReportManager.Builder
     */
    public Builder setStateContext(JobworkerStateContext context) {
      stateContext = context;
      return this;
    }

    /**
     * Adds publisher for the corresponding report.
     *
     * @param report report for which publisher needs to be added
     *
     * @return JobworkerReportManager.Builder
     */
    public Builder addPublisherFor(Class<? extends Message> report) {
      reportPublishers.add(publisherFactory.getPublisherFor(report));
      return this;
    }

    /**
     * Adds new JobworkerReportPublisher to the JobworkerReportManager.
     *
     * @param publisher JobworkerReportPublisher
     *
     * @return JobworkerReportManager.Builder
     */
    public Builder addPublisher(JobworkerReportPublisher publisher) {
      reportPublishers.add(publisher);
      return this;
    }

    public Builder addThreadNamePrefix(String threadPrefix) {
      this.threadNamePrefix = threadPrefix;
      return this;
    }

    /**
     * Build and returns JobworkerReportManager.
     *
     * @return {@link JobworkerReportManager}
     */
    public JobworkerReportManager build() {
      Preconditions.checkNotNull(stateContext);
      return new JobworkerReportManager(
          stateContext, reportPublishers, threadNamePrefix, conf);
    }
  }
}
