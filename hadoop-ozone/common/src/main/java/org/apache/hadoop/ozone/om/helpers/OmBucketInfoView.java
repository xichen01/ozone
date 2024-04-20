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

import org.apache.hadoop.hdds.protocol.StorageType;

@SuppressWarnings("checkstyle:MissingJavadocType")
public class OmBucketInfoView {
  private final OmBucketInfo omBucketInfo;

  public OmBucketInfoView(OmBucketInfo omBucketInfo) {
    this.omBucketInfo = omBucketInfo;
  }

  public String getVolumeName() {
    return omBucketInfo.getVolumeName();
  }

  /**
   * Returns the Bucket Name.
   * @return String
   */
  public String getBucketName() {
    return omBucketInfo.getBucketName();
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public boolean getIsVersionEnabled() {
    return omBucketInfo.getIsVersionEnabled();
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return omBucketInfo.getStorageType();
  }

  /**
   * Returns creation time.
   *
   * @return long
   */
  public long getCreationTime() {
    return omBucketInfo.getCreationTime();
  }

  /**
   * Returns the Bucket Layout.
   *
   * @return BucketLayout.
   */
  public BucketLayout getBucketLayout() {
    return omBucketInfo.getBucketLayout();
  }

  public String getSourceVolume() {
    return omBucketInfo.getSourceVolume();
  }

  public String getSourceBucket() {
    return omBucketInfo.getSourceBucket();
  }

  public long getUsedBytes() {
    return omBucketInfo.getUsedBytes();
  }

  public long getUsedNamespace() {
    return omBucketInfo.getUsedNamespace();
  }

  public boolean isLink() {
    return omBucketInfo.isLink();
  }

  public String getOwner() {
    return omBucketInfo.getOwner();
  }

  public long getQuotaInBytes() {
    return omBucketInfo.getQuotaInBytes();
  }

  public long getQuotaInNamespace() {
    return omBucketInfo.getQuotaInNamespace();
  }

  public long getObjectID() {
    return omBucketInfo.getObjectID();
  }
}
