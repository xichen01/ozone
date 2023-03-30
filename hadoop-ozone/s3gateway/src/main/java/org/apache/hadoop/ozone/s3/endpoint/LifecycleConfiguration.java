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

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLCExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleRuleAndOperator;

import javax.ws.rs.WebApplicationException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Request for put bucket lifecycle configuration.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "LifecycleConfiguration",
    namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class LifecycleConfiguration {
  @XmlElement(name = "Rule")
  private List<Rule> rules = new ArrayList<>();

  public List<Rule> getRules() {
    return rules;
  }

  public void setRules(List<Rule> rules) {
    this.rules = rules;
  }

  /**
   * Entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Rule")
  public static class Rule {
    @XmlElement(name = "ID")
    private String id;

    @XmlElement(name = "Status")
    private String status;

    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Expiration")
    private Expiration expiration;

    @XmlElement(name = "Filter")
    private Filter filter;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public Expiration getExpiration() {
      return expiration;
    }

    public void setExpiration(Expiration expiration) {
      this.expiration = expiration;
    }

    public Filter getFilter() {
      return filter;
    }

    public void setFilter(Filter filter) {
      this.filter = filter;
    }
  }

  /**
   * Entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Expiration")
  public static class Expiration {
    @XmlElement(name = "Days")
    private Integer days;

    @XmlElement(name = "Date")
    private String date;


    public Integer getDays() {
      return days;
    }

    public void setDays(Integer days) {
      this.days = days;
    }

    public String getDate() {
      return date;
    }

    public void setDate(String date) {
      this.date = date;
    }
  }

  /**
   * Entity for child element Tag.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Tag")
  public static class Tag {
    @XmlElement(name = "Key")
    private String key;

    @XmlElement(name = "Value")
    private String value;

    public Tag() {
    }

    public Tag(String key, String value) {
      this.value = value;
      this.key = key;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  /**
   * Entity for child element AndOperator.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "And")
  public static class AndOperator {
    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Tag")
    private List<Tag> tags = null;

    public List<Tag> getTags() {
      return tags;
    }

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public void setTags(List<Tag> tags) {
      this.tags = tags;
    }
  }

  /**
   * Entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Filter")
  public static class Filter {
    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Tag")
    private Tag tag = null;

    @XmlElement(name = "And")
    private AndOperator andOperator;

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public Tag getTag() {
      return tag;
    }

    public void setTag(Tag tag) {
      this.tag = tag;
    }

    public AndOperator getAndOperator() {
      return andOperator;
    }

    public void setAndOperator(
        AndOperator andOperator) {
      this.andOperator = andOperator;
    }

  }

  /**
   * Returns OmLifecycleConfiguration object from existing
   * LifecycleConfiguration, Also validates the expiration date format if it
   * exists.
   *
   * @param volumeName
   * @param bucketName
   * @return OmLifecycleConfiguration
   * @throws DateTimeParseException if the expiration cannot be parsed.
   */
  public OmLifecycleConfiguration toOmLifecycleConfiguration(String volumeName, String bucketName)
      throws DateTimeParseException, IllegalArgumentException {
    OmLifecycleConfiguration.Builder builder =
        new OmLifecycleConfiguration.Builder()
            .setVolume(volumeName)
            .setBucket(bucketName);

    for (LifecycleConfiguration.Rule r: getRules()) {
      validateConfigurationRules(r);
      OmLCRule.Builder b = new OmLCRule.Builder()
          .setEnabled(r.getStatus().equals("Enabled"))
          .setId(r.getId())
          .setPrefix(r.getPrefix());

      if (r.getExpiration() != null) {
        OmLCExpiration.Builder e = new OmLCExpiration.Builder();

        if (r.getExpiration().getDays() != null) {
          e.setDays(r.getExpiration().getDays());
        }

        if (r.getExpiration().getDate() != null) {
          LocalDate.parse(r.getExpiration().getDate(),
              DateTimeFormatter.ISO_DATE_TIME);
          e.setDate(r.getExpiration().getDate());
        }

        b.setExpiration(e.build());
      }

      Filter filter = r.getFilter();
      if (filter != null) {
        OmLCFilter.Builder f = new OmLCFilter.Builder();
        if (filter.getPrefix() != null) {
          f.setPrefix(r.getFilter().getPrefix());
        }
        if (filter.getTag() != null) {
          f.setTag(filter.getTag().getKey(), filter.getTag().getValue());
        }

        AndOperator andOperator = filter.getAndOperator();
        if (andOperator != null) {
          OmLifecycleRuleAndOperator.Builder a = new OmLifecycleRuleAndOperator.Builder();
          if (andOperator.getPrefix() != null) {
            a.setPrefix(andOperator.getPrefix());
          }
          if (andOperator.getTags() != null) {
            Map<String, String> tags = andOperator.getTags().stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
            a.setTags(tags);
          }
          f.setAndOperator(a.build());
        }
        b.setFilter(f.build());
      }
      builder.addRule(b.build());
    }

    return builder.build();
  }

  // {@link OzoneManagerProtocolPB}
  public void validateConfigurationRules(LifecycleConfiguration.Rule rule)
      throws WebApplicationException, IllegalArgumentException {
    // Refer to: https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
    Filter filter = rule.getFilter();
    if (filter == null) {
      return;
    }

    if (rule.getPrefix() != null) {
      /** {@link TestLifecycleConfigurationPut#usePrefixWithFilter()} */
      throw new WebApplicationException("Filter and Prefix cannot be used together.");
    }

    if (filter.getTag() != null && filter.getPrefix() != null) {
      /** {@link TestLifecycleConfigurationPut#usePrefixTagWithoutAndOperator()} */
      throw new WebApplicationException("Tag and Prefix cannot be used together outside And operator.");
    }

    AndOperator andOperator = filter.getAndOperator();
    if (andOperator != null) {
      // When the AndOperator is used, the content cannot be empty and must contain at least two
      // elements. This includes either [0, 1] Prefix, and [1, n) Tags. The operator must not contain
      // only a single Prefix without any Tags, or only one pair of Tags without a Prefix.
      // This ensures that the AndOperator always combines multiple criteria.
      if (filter.getPrefix() != null || filter.getTag() != null) {
        /** {@link TestLifecycleConfigurationPut#usePrefixAndOperatorCoExistInFilter()} */
        throw new WebApplicationException("AndOperator cannot co-exist with Tag or Prefix.");
      }

      if (andOperator.getPrefix() == null && andOperator.getTags() == null) {
        /** {@link TestLifecycleConfigurationPut#useEmptyAndOperator()} */
        throw new WebApplicationException("AndOperator must not be empty and must contain two filter.");
      }

      if (andOperator.getPrefix() == null && andOperator.getTags().size() == 1) {
        /** {@link TestLifecycleConfigurationPut#useEmptyAndOperator()} */
        throw new WebApplicationException("AndOperator with only one tag is not allowed.");
      }

      if (andOperator.getPrefix() != null && andOperator.getTags() == null) {
        /** {@link TestLifecycleConfigurationPut#useAndOperatorOnlyOnePrefix()} */
        throw new WebApplicationException("AndOperator with only a prefix is not allowed.");
      }

      Set<String> tagKeys = new HashSet<>();
      for (Tag tag : andOperator.getTags()) {
        if (tagKeys.contains(tag.getKey())) {
          /** {@link TestLifecycleConfigurationPut#useDuplicateTagInAndOperator()} */
          throw new IllegalArgumentException("Duplicate Tag Keys are not allowed.");
        } else {
          tagKeys.add(tag.getKey());
        }
      }
    }
  }

  /**
   * Creates a LifecycleConfiguration instance (xml representation) from an
   * OzoneLifecycleConfiguration instance.
   * @param ozoneLifecycleConfiguration
   * @return
   */
  public static LifecycleConfiguration fromOzoneLifecycleConfiguration(
      OzoneLifecycleConfiguration ozoneLifecycleConfiguration) {
    List<LifecycleConfiguration.Rule> rules = new ArrayList<>();
    for (OzoneLifecycleConfiguration.OzoneLCRule r:
        ozoneLifecycleConfiguration.getRules()) {
      LifecycleConfiguration.Rule rule = new LifecycleConfiguration.Rule();
      if (r.getExpiration() != null) {
        LifecycleConfiguration.Expiration e =
            new LifecycleConfiguration.Expiration();
        String rDate = r.getExpiration().getDate();
        if (rDate != null && !rDate.isEmpty()) {
          e.setDate(rDate);
        }
        if (r.getExpiration().getDays() > 0) {
          e.setDays(r.getExpiration().getDays());
        }
        rule.setExpiration(e);
      }
      if (r.getFilter() != null) {
        LifecycleConfiguration.Filter f = new LifecycleConfiguration.Filter();
        f.setPrefix(r.getFilter().getPrefix());
        if (r.getFilter().getTag() != null) {
          f.setTag(new Tag(r.getFilter().getTag().getKey(), r.getFilter().getTag().getValue()));
        }
        if (r.getFilter().getAndOperator() != null) {
          LifecycleConfiguration.AndOperator a = new LifecycleConfiguration.AndOperator();
          a.setPrefix(r.getFilter().getAndOperator().getPrefix());
          a.setTags(r.getFilter().getAndOperator().getTags().entrySet().stream()
              .map(tag -> new Tag(tag.getKey(), tag.getValue())).collect(Collectors.toList()));
          f.setAndOperator(a);
        }
        rule.setFilter(f);
      }
      if (r.getPrefix() != null) {
        rule.setPrefix(r.getPrefix());
      }
      rule.setId(r.getId());
      rule.setStatus(r.getStatus());
      rules.add(rule);
    }

    LifecycleConfiguration lifecycleConfiguration =
        new LifecycleConfiguration();
    lifecycleConfiguration.setRules(rules);

    return lifecycleConfiguration;
  }
}
