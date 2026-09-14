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

package org.apache.hadoop.ozone.common;

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/** Thrown for checksum errors. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OzoneChecksumException extends IOException {
  /** Describes the checksum failure. */
  public enum FailureType {
    CHECKSUM_MISMATCH,
    SOURCE_CHECKSUM_TYPE_UNSUPPORTED,
    WRITE_CHECKSUM_TYPE_UNSUPPORTED,
    UNRECOGNIZED_TYPE,
    OTHER
  }

  private final FailureType failureType;

  /**
   * OzoneChecksumException to throw with custom message.
   */
  public OzoneChecksumException(String message) {
    super(message);
    this.failureType = FailureType.OTHER;
  }

  public OzoneChecksumException(String message, Throwable cause) {
    super(message, cause);
    this.failureType = FailureType.OTHER;
  }

  public OzoneChecksumException(FailureType failureType, String message) {
    super(message);
    this.failureType = failureType;
  }

  public FailureType getFailureType() {
    return failureType;
  }
}
