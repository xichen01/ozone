/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.jobworker;

import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.NodeReportFromJobworker;

/**
 * Class that acts as the namespace for all OM jobworker Events.
 */
public final class OMJobworkerEvents {

  /**
   * Process the node report sent by Jobworker.
   */
  public static final TypedEvent<NodeReportFromJobworker>
      JW_NODE_REPORT = new TypedEvent<>(NodeReportFromJobworker.class,
          "JW_Node_Report");

  /**
   * Private Ctor. Never Constructed.
   */
  private OMJobworkerEvents() {
  }
}
