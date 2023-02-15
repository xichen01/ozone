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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that encapsulates lifecycle configuration.
 */
public class OmLifecycleConfiguration {
  public static final int LC_MAX_RULES = 1000;
  private String bucket;
  private String owner;
  private List<OmLCRule> rules;

  OmLifecycleConfiguration(String bucket, String owner, List<OmLCRule> rules) {
    this.bucket = bucket;
    this.owner = owner;
    this.rules = rules;
  }

  public List<OmLCRule> getRules() {
    return rules;
  }

  public void setRules(List<OmLCRule> rules) {
    this.rules = rules;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public boolean isValid() {
    // TODO: consider refactor and raise different exception based on the issue.
    if (StringUtils.isEmpty(bucket)) {
      return false;
    }

    if (StringUtils.isEmpty(owner)) {
      return false;
    }

    if (rules.isEmpty() || rules.size() > LC_MAX_RULES) {
      return false;
    }

    if (!hasNoDuplicateID()) {
      return false;
    }

    if (rules.stream().anyMatch(r -> !r.isValid())) {
      return false;
    }

    return true;
  }

  private boolean hasNoDuplicateID() {
    return rules.size() == rules.stream()
        .map(OmLCRule::getId)
        .collect(Collectors.toSet())
        .size();
  }

  @Override
  public String toString() {
    return "OmLifecycleConfiguration{" +
        "bucket='" + bucket + '\'' +
        ", owner='" + owner + '\'' +
        '}';
  }

  /**
   * Builder of OmLifecycleConfiguration.
   */
  public static class Builder {
    private String bucket;
    private String owner;
    private List<OmLCRule> rules;

    public Builder() {
      this.bucket = "";
      this.owner = "";
      this.rules = new ArrayList<>();
    }

    public Builder setBucket(String bucketName) {
      this.bucket = bucketName;
      return this;
    }

    public Builder setOwner(String ownerName) {
      this.owner = ownerName;
      return this;
    }

    public Builder addRule(OmLCRule rule) {
      this.rules.add(rule);
      return this;
    }

    public Builder setRules(List<OmLCRule> lcRules) {
      this.rules = lcRules;
      return this;
    }

    public OmLifecycleConfiguration build() {
      return new OmLifecycleConfiguration(bucket, owner, rules);
    }
  }
}
