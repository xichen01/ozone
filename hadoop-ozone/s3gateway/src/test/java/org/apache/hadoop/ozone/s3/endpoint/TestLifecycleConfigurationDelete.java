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

import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static java.net.HttpURLConnection.*;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_LIFECYCLE_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Testing for DeleteBucketLifecycleConfiguration.
 */
public class TestLifecycleConfigurationDelete {
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
  public void testDeleteNonExistentLifecycleConfiguration()
      throws Exception {
    try {
      bucketEndpoint.delete("bucket1", "");
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_LIFECYCLE_CONFIGURATION.getCode(),
              ex.getCode());
    }
  }

  @Test
  public void testCreateInvalidLifecycleConfiguration() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, null, "", null, getBody());
    Response r = bucketEndpoint.delete(bucketName, "");

    assertEquals(HTTP_NO_CONTENT, r.getStatus());

    try {
      // Make sure it was deleted.
      bucketEndpoint.get(bucketName, null, null, null, 0, null, null,
          null, null, null, null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_LIFECYCLE_CONFIGURATION.getCode(),
          ex.getCode());
    }
  }

  private static InputStream getBody() {
    String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Expiration><Days>30</Days></Expiration>" +
        "<Status>Enabled</Status>" +
        "</Rule>" +
        "</LifecycleConfiguration>");

    return new ByteArrayInputStream(xml.getBytes());
  }
}
