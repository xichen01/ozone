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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;

/**
 * RpcClientStub for Lifecycle configuration testing.
 */
public class RpcClientStub extends RpcClient {
  private Map<String, OzoneLifecycleConfiguration> lifecyclesMap;
  private Set<String> existingBuckets;

  public RpcClientStub(ConfigurationSource conf,
      String omServiceId) throws IOException {
    super(conf, omServiceId);
    lifecyclesMap = new HashMap<>();
    existingBuckets = new HashSet<>();
  }

  public RpcClientStub(List<String> buckets) throws IOException {
    this(new OzoneConfiguration(), null);
    lifecyclesMap = new HashMap<>();
    existingBuckets = new HashSet<>();
    existingBuckets.addAll(buckets);
  }

  @Override
  public void createLifecycleConfiguration(
      OmLifecycleConfiguration lifecycleConfiguration) throws IOException {
    if (!existingBuckets.contains(getKey(lifecycleConfiguration))) {
      throw new OMException("Bucket doesn't exist", BUCKET_NOT_FOUND);
    }
    lifecyclesMap.put(getKey(lifecycleConfiguration),
        toOzoneLifecycleConfiguration(lifecycleConfiguration));
  }

  @Override
  public OzoneLifecycleConfiguration getLifecycleConfiguration(
      String volumeName, String bucketName) throws IOException {
    OzoneLifecycleConfiguration lcc = lifecyclesMap.get(getKey(volumeName,
        bucketName));
    if (lcc == null) {
      throw new OMException("Lifecycle configuration not found",
          LIFECYCLE_CONFIGURATION_NOT_FOUND);
    }
    return lcc;
  }

  @Override
  public void deleteLifecycleConfiguration(String volumeName, String bucketName)
      throws IOException {
    String key = getKey(volumeName, bucketName);
    if (!lifecyclesMap.containsKey(key)) {
      throw new OMException("Lifecycle configurations does not exist",
          OMException.ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND);
    }
    lifecyclesMap.remove(key);
  }

  private static OzoneLifecycleConfiguration toOzoneLifecycleConfiguration(
      OmLifecycleConfiguration omLifecycleConfiguration) {
    List<OzoneLifecycleConfiguration.OzoneLCRule> rules = new ArrayList<>();

    for (OmLCRule r: omLifecycleConfiguration.getRules()) {
      OzoneLifecycleConfiguration.OzoneLCExpiration e = null;
      OzoneLifecycleConfiguration.OzoneLCFilter f = null;

      if (r.getExpiration() != null) {
        e = new OzoneLifecycleConfiguration.OzoneLCExpiration(
            r.getExpiration().getDays(), r.getExpiration().getDate());
      }
      if (r.getFilter() != null) {
        OzoneLifecycleConfiguration.LifecycleAndOperator andOperator = null;
        if (r.getFilter().getAndOperator() != null) {
          andOperator = new OzoneLifecycleConfiguration.LifecycleAndOperator(r.getFilter().getAndOperator()
              .getTags(), r.getFilter().getAndOperator().getPrefix());
        }
        f = new OzoneLifecycleConfiguration.OzoneLCFilter(
            r.getFilter().getPrefix(), r.getFilter().getTag(), andOperator);
      }

      rules.add(new OzoneLifecycleConfiguration.OzoneLCRule(r.getId(),
          r.getPrefix(), (r.isEnabled() ? "Enabled" : "Disabled"), e, f));
    }

    return new OzoneLifecycleConfiguration(
        omLifecycleConfiguration.getVolume(),
        omLifecycleConfiguration.getBucket(),
        omLifecycleConfiguration.getCreationTime(), rules);
  }

  private static String getKey(OmLifecycleConfiguration lcc) {
    return getKey(lcc.getVolume(), lcc.getBucket());
  }
  private static String getKey(String volumeName, String bucketName) {
    return "/" + volumeName + "/" + bucketName;
  }
}
