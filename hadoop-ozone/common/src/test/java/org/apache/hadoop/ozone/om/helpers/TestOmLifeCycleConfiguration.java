/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test lifecycle configuration related entities.
 */
public class TestOmLifeCycleConfiguration {

  @Test
  public void testCreateValidOmLCExpiration() {
    OmLCExpiration exp1 = new OmLCExpiration.Builder()
            .setDays(30)
            .build();
    assertTrue(exp1.isValid(), "Expected a valid expiration with days only passed");

    OmLCExpiration exp2 = new OmLCExpiration.Builder()
            .setDate("10/10/2025")
            .build();
    assertTrue(exp2.isValid(), "Expected a valid expiration with a date");
  }

  @Test
  public void testCreateInValidOmLCExpiration() {
    OmLCExpiration exp1 = new OmLCExpiration.Builder()
            .setDays(30)
            .setDate("10/10/2025")
            .build();
    assertFalse(exp1.isValid(), "Expected invalid expiration with days and date passed");

    OmLCExpiration exp2 = new OmLCExpiration.Builder()
            .setDays(-1)
            .build();
    assertFalse(exp2.isValid(), "Expected invalid expiration with negative days");

    OmLCExpiration exp3 = new OmLCExpiration.Builder()
            .setDate(null)
            .build();
    assertFalse(exp3.isValid(), "Expected invalid expiration with null date");

    OmLCExpiration exp4 = new OmLCExpiration.Builder()
            .setDate("")
            .build();
    assertFalse(exp4.isValid(), "Expected invalid expiration with empty date");

    OmLCExpiration exp5 = new OmLCExpiration.Builder()
            .build();
    assertFalse(exp5.isValid(), "Expected invalid expiration with 0 days and empty date");
  }

  @Test
  public void testCreateValidOmLCRule() {
    OmLCExpiration exp = new OmLCExpiration.Builder()
            .setDays(30)
            .build();

    OmLCRule r1 = new OmLCRule.Builder()
            .setId("remove Spark logs after 30 days")
            .setEnabled(true)
            .setPrefix("/spark/logs")
            .setExpiration(exp)
            .build();
    assertTrue(r1.isValid(), "Expected a valid rule");

    // Empty id should generate a 48 (default) bit one.
    OmLCRule r2 = new OmLCRule.Builder()
            .setEnabled(true)
            .setExpiration(exp)
            .build();

    assertTrue(r2.isValid(), "Expected a valid rule");
    assertEquals(OmLCRule.LC_ID_LENGTH, r2.getId().length(),
            "Expected a " + OmLCRule.LC_ID_LENGTH + " length generated ID"
    );
  }

  @Test
  public void testCreateInValidOmLCRule() {
    OmLCExpiration exp = new OmLCExpiration.Builder()
            .setDays(30)
            .build();

    char[] id = new char[OmLCRule.LC_ID_MAX_LENGTH + 1];
    Arrays.fill(id, 'a');

    OmLCRule r1 = new OmLCRule.Builder()
            .setId(new String(id))
            .setExpiration(exp)
            .build();
    assertFalse(r1.isValid(), "Expected invalid rule with a long id");

    OmLCRule r2 = new OmLCRule.Builder()
            .setId("remove Spark logs after 30 days")
            .setEnabled(true)
            .setPrefix("/spark/logs")
            .setExpiration(null)
            .build();
    assertFalse(r2.isValid(), "Expected invalid rule with a null expiration");

    OmLCRule r3 = new OmLCRule.Builder()
            .setId("remove Spark logs after 30 days")
            .setEnabled(true)
            .setPrefix("/spark/logs")
            .setExpiration(new OmLCExpiration.Builder()
                    .setDays(30)
                    .setDate("10/10/2025")
                    .build())
            .build();
    assertFalse(r3.isValid(), "Expected invalid rule with an invalid expiration");
  }

  @Test
  public void testCreateValidLCConfiguration() {
    OmLifecycleConfiguration lcc = new OmLifecycleConfiguration.Builder()
            .setBucket("/s3v/spark")
            .setOwner("ozone")
            .setRules(Collections.singletonList(new OmLCRule.Builder()
                    .setId("spark logs")
                    .setExpiration(new OmLCExpiration.Builder()
                            .setDays(30)
                            .build())
                    .build()))
            .build();

    assertTrue(lcc.isValid(), "Expected a valid lifecycle configuration");
  }

  @Test
  public void testCreateInValidLCConfiguration() {
    OmLCRule rule = new OmLCRule.Builder()
            .setId("spark logs")
            .setExpiration(new OmLCExpiration.Builder().setDays(30).build())
            .build();

    List<OmLCRule> rules = Collections.singletonList(rule);

    OmLifecycleConfiguration lcc1 = new OmLifecycleConfiguration.Builder()
            .setOwner("ozone")
            .setRules(rules)
            .build();
    assertFalse(lcc1.isValid(),
            "Expected invalid lifecycle configuration missing bucket");

    OmLifecycleConfiguration lcc2 = new OmLifecycleConfiguration.Builder()
            .setBucket("/s3v/spark")
            .setRules(rules)
            .build();
    assertFalse(lcc2.isValid(),
            "Expected invalid lifecycle configuration missing owner");

    OmLifecycleConfiguration lcc3 = new OmLifecycleConfiguration.Builder()
            .setBucket("/s3v/spark")
            .setOwner("ozone")
            .setRules(Collections.emptyList())
            .build();
    assertFalse(lcc3.isValid(),
            "Expected invalid lifecycle configuration missing rules");

    List<OmLCRule> rules4 = new ArrayList<>(
        OmLifecycleConfiguration.LC_MAX_RULES + 1);
    for (int i = 0; i < OmLifecycleConfiguration.LC_MAX_RULES + 1; i++) {
      OmLCRule r = new OmLCRule.Builder()
          .setId(Integer.toString(i))
          .setExpiration(new OmLCExpiration.Builder().setDays(30).build())
          .build();
      rules4.add(r);
    }
    OmLifecycleConfiguration lcc4 = new OmLifecycleConfiguration.Builder()
            .setBucket("/s3v/spark")
            .setOwner("ozone")
            .setRules(rules4)
            .build();
    assertFalse(lcc4.isValid(),
            "Expected invalid lifecycle configuration exceeding "
                    + OmLifecycleConfiguration.LC_MAX_RULES + " rules");

    List<OmLCRule> rules5 = rules4.subList(0, rules4.size() - 2);
    // last rule is invalid.
    rules5.add(new OmLCRule.Builder().build());
    OmLifecycleConfiguration lcc5 = new OmLifecycleConfiguration.Builder()
            .setBucket("/s3v/spark")
            .setOwner("ozone")
            .setRules(rules5)
            .build();
    assertFalse(lcc5.isValid(),
            "Expected invalid lifecycle configuration with an invalid rule");

    OmLifecycleConfiguration lcc6 = new OmLifecycleConfiguration.Builder()
        .setBucket("/s3v/spark")
        .setOwner("ozone")
        .setRules(Collections.nCopies(2, rule))
        .build();
    assertFalse(lcc6.isValid(),
        "Expected invalid lifecycle configuration with duplicated ruleID");
  }
}
