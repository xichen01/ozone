/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Test Http request log appender.
 */
public class TestHttpRequestLogAppender {

  @Test
  public void testParameterPropagation() {

    HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender();
    requestLogAppender.setFilename("jetty-namenode-yyyy_mm_dd.log");
    requestLogAppender.setRetainDays(17);
    assertEquals("jetty-namenode-yyyy_mm_dd.log",
        requestLogAppender.getFilename(), "Filename mismatch");
    assertEquals(17, requestLogAppender.getRetainDays(),
        "Retain days mismatch");
  }
}
