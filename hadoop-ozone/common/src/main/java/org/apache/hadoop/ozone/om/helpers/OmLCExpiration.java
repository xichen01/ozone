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
 * A class that encapsulates lifecycle rule expiration.
 */
public class OmLCExpiration {
  private int days;
  private String date;

  OmLCExpiration(int days, String date) {
    this.days = days;
    this.date = date;
  }

  public int getDays() {
    return days;
  }

  public void setDays(int days) {
    this.days = days;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public boolean isValid() {
    boolean hasDays = days > 0;
    boolean hasDate = date != null && !date.isEmpty();
    // Either days or date should be used but not both
    return hasDays ^ hasDate;
  }

  @Override
  public String toString() {
    return "OmLCExpiration{" +
            "days=" + days +
            ", date='" + date + '\'' +
            '}';
  }

  /**
   * Builder of OmLCExpiration.
   */
  public static class Builder {
    private int days;
    private String date = "";

    public Builder setDays(int lcDays) {
      this.days = lcDays;
      return this;
    }

    public Builder setDate(String lcDate) {
      this.date = lcDate;
      return this;
    }

    public OmLCExpiration build() {
      return new OmLCExpiration(days, date);
    }
  }
}
