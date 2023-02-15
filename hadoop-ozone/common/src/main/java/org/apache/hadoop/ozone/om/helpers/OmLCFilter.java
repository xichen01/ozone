/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

/**
 * A class that encapsulates lifecycle rule filter.
 * At the moment only prefix is supported in filter.
 */
public class OmLCFilter {
  private String prefix;

  OmLCFilter(String prefix) {
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public String toString() {
    return "OmLCFilter{" +
            "prefix='" + prefix + '\'' +
            '}';
  }

  /**
   * Builder of OmLCFilter.
   */
  public static class Builder {
    private String prefix = "";

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
      return this;
    }

    public OmLCFilter build() {
      return new OmLCFilter(prefix);
    }
  }

}
