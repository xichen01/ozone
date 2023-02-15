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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * A class that encapsulates lifecycle rule.
 */
public class OmLCRule {
  // TODO: pull to another common-configurable location.
  public static final int LC_ID_LENGTH = 48;
  public static final int LC_ID_MAX_LENGTH = 255;

  private String id;
  private String prefix;
  private boolean enabled;
  // TODO: Implement other rule specifications (non-current-expiration,
  //  multipart-expiration,
  //  delete-marker-expiration).
  // current expiration
  private OmLCExpiration expiration;
  private OmLCFilter filter;

  OmLCRule(String id, String prefix, boolean enabled,
           OmLCExpiration expiration, OmLCFilter filter) {
    // Expiration is not required according to Amazon,
    // But in our case the rule consists of expiration only
    // Without it, the rule is useless.
    this.id = id;
    this.prefix = prefix;
    this.enabled = enabled;
    this.expiration = expiration;
    this.filter = filter;

    if (StringUtils.isEmpty(this.id)) {
      this.id = RandomStringUtils.randomAlphanumeric(LC_ID_LENGTH);
    }
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public OmLCExpiration getExpiration() {
    return expiration;
  }

  public void setExpiration(OmLCExpiration expiration) {
    this.expiration = expiration;
  }

  public OmLCFilter getFilter() {
    return filter;
  }

  public void setFilter(OmLCFilter filter) {
    this.filter = filter;
  }

  public boolean isValid() {
    // TODO: consider refactor and raise different exception based on the issue.
    if (id.length() > LC_ID_MAX_LENGTH) {
      return false;
    }

    if (expiration == null || !expiration.isValid()) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return "OmLCRule{" +
            "id='" + id + '\'' +
            ", prefix='" + prefix + '\'' +
            ", enabled=" + enabled +
            '}';
  }

  /**
   * Builder of OmLCRule.
   */
  public static class Builder {
    private String id = "";
    private String prefix = "";
    private boolean enabled;
    // current expiration
    private OmLCExpiration expiration;
    private OmLCFilter filter;

    public Builder setId(String lcId) {
      this.id = lcId;
      return this;
    }

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
      return this;
    }

    public Builder setEnabled(boolean lcEnabled) {
      this.enabled = lcEnabled;
      return this;
    }

    public Builder setExpiration(OmLCExpiration lcExpiration) {
      this.expiration = lcExpiration;
      return this;
    }

    public Builder setFilter(OmLCFilter lcFilter) {
      this.filter = lcFilter;
      return this;
    }

    public OmLCRule build() {
      return new OmLCRule(id, prefix, enabled, expiration, filter);
    }
  }
}
