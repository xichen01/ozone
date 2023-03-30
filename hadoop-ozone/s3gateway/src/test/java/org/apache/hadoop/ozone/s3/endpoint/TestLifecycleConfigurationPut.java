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

import org.apache.hadoop.ozone.client.ObjectStoreStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.RpcClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.net.HttpURLConnection.*;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

/**
 * Testing for PutBucketLifecycleConfiguration.
 */
public class TestLifecycleConfigurationPut {
  private static List<String> buckets = Arrays.asList(
      "/s3v/bucket1",
      "/s3v/bucket2",
      "/s3v/bucket3"
  );

  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {
    //Create client stub and object store stub.
    ClientProtocol proxy = new RpcClientStub(buckets);
    ObjectStoreStub objectStoreStub = Mockito.mock(ObjectStoreStub.class);
    Mockito.when(objectStoreStub.getClientProxy()).thenReturn(proxy);
    clientStub = new OzoneClientStub(objectStoreStub);

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);
  }

  @Test
  public void testLifecycleConfigurationFailWithEmptyBody() throws Exception {
    try {
      bucketEndpoint.put("bucketname", null, "", null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
      assertEquals(MALFORMED_XML.getCode(), ex.getCode());
    }
  }

  @Test
  public void testLifecycleConfigurationFailWithNonExistentBucket()
      throws Exception {
    try {
      bucketEndpoint.put("nonexistentbucket", null, "", null, onePrefix(true));
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }

  @Test
  public void testCreateInvalidLifecycleConfiguration() throws Exception {
    testInvalidLifecycleConfiguration(() -> onePrefix(false), HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::usePrefixTagWithoutAndOperator, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidLifecycleConfiguration(this::usePrefixAndOperatorCoExistInFilter, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidLifecycleConfiguration(this::usePrefixFilterCoExist, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidLifecycleConfiguration(this::useAndOperatorOnlyOnePrefix, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidLifecycleConfiguration(this::useAndOperatorOnlyOneTag, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidLifecycleConfiguration(this::useEmptyAndOperator, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidLifecycleConfiguration(this::useDuplicateTagInAndOperator, HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
  }

  private void testInvalidLifecycleConfiguration(Supplier<InputStream> inputStream,
      int expectedHttpCode, String expectedErrorCode) throws Exception {
    try {
      bucketEndpoint.put("bucket1", null, "", null, inputStream.get());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(expectedHttpCode, ex.getHttpCode());
      assertEquals(expectedErrorCode, ex.getCode());
    }
  }

  @Test
  public void testCreateInvalidExpirationDateLCC() throws Exception {
    try {
      String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
          ".com/doc/2006-03-01/\">" +
          "<Rule>" +
          "<ID>remove logs after 30 days</ID>" +
          "<Prefix>prefix/</Prefix>" +
          "<Status>Enabled</Status>" +
          "<Expiration><Date>2023-03-03</Date></Expiration>" +
          "</Rule>" +
          "</LifecycleConfiguration>");

      bucketEndpoint.put("bucket1", null, "", null,
          new ByteArrayInputStream(xml.getBytes()));
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
      assertEquals(INVALID_REQUEST.getCode(), ex.getCode());
    }
  }

  @Test
  public void testCreateValidLifecycleConfiguration() throws Exception {
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, onePrefix(true)).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, emptyPrefix()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, oneTag()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, twoTagsInAndOperator()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, onePrefixTwoTagsInAndOperator()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, onePrefixTwoTags()).getStatus());
  }

  @Test
  public void testCreateLifecycleConfigurationFailsWithNonBucketOwner()
      throws Exception {
    bucketEndpoint.createS3Bucket("bucket1");
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);
    when(httpHeaders.getHeaderString("x-amz-expected-bucket-owner"))
        .thenReturn("anotheruser");

    try {
      bucketEndpoint.put("bucket1", null, "", httpHeaders, onePrefix(true));
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_FORBIDDEN, ex.getHttpCode());
      assertEquals(ACCESS_DENIED.getCode(), ex.getCode());
    }
  }

  private static InputStream onePrefix(boolean isValid) {
    String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Status>Enabled</Status>");

    if (isValid) {
      xml += "<Expiration><Days>30</Days></Expiration>";
    }

    xml += ("</Rule>" +
        "</LifecycleConfiguration>");

    return new ByteArrayInputStream(xml.getBytes());
  }

  private static InputStream twoTagsInAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private static InputStream onePrefixTwoTagsInAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix></Prefix>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private static InputStream onePrefixTwoTags() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix>key-prefix</Prefix>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private static InputStream emptyPrefix() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Prefix></Prefix>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private static InputStream oneTag() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Tag>" +
            "                 <Key>key1</Key>" +
            "                 <Value>value1</Value>" +
            "             </Tag>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream usePrefixTagWithoutAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Prefix>key-prefix</Prefix>" +
            "             <Tag>" +
            "                 <Key>key2</Key>" +
            "                 <Value>value1</Value>" +
            "             </Tag>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream usePrefixAndOperatorCoExistInFilter() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Prefix>key-prefix</Prefix>" +
            "             <And>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream useAndOperatorOnlyOnePrefix() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix>key-prefix</Prefix>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream useAndOperatorOnlyOneTag() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream useEmptyAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream usePrefixFilterCoExist() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Prefix>key-prefix</Prefix>" +
            "         <Filter>" +
            "             <Tag>" +
            "                 <Key>key1</Key>" +
            "                 <Value>value1</Value>" +
            "             </Tag>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }

  private InputStream useDuplicateTagInAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix>key-prefix</Prefix>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes());
  }



}
