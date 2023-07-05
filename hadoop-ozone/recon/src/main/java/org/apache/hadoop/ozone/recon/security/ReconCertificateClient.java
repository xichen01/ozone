/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.security;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.client.CommonCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getX509Certificate;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CSR_ERROR;

/**
 * Certificate client for Recon.
 */
public class ReconCertificateClient  extends CommonCertificateClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconCertificateClient.class);

  public static final String COMPONENT_NAME = "recon";
  private final String clusterID;
  private final String reconID;

  public ReconCertificateClient(
      SecurityConfig config,
      SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient,
      ReconStorageConfig storage,
      Consumer<String> saveCertIdCallback,
      Runnable shutdownCallback) {
    super(config, scmSecurityClient, LOG, storage.getReconCertSerialId(),
        COMPONENT_NAME, saveCertIdCallback, shutdownCallback);
    this.clusterID = storage.getClusterID();
    this.reconID = storage.getReconId();
  }

  @Override
  public CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException {
    LOG.info("Creating CSR for Recon.");
    try {
      CertificateSignRequest.Builder builder = super.getCSRBuilder();
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();
      String subject = UserGroupInformation.getCurrentUser()
          .getShortUserName() + "@" + hostname;

      builder.setCA(false)
          .setKey(new KeyPair(getPublicKey(), getPrivateKey()))
          .setConfiguration(getSecurityConfig())
          .setSubject(subject);

      return builder;
    } catch (Exception e) {
      LOG.error("Failed to get hostname or current user", e);
      throw new CertificateException("Failed to get hostname or current user",
          e, CSR_ERROR);
    }
  }

  @Override
  public String signAndStoreCertificate(PKCS10CertificationRequest csr,
      Path certificatePath, boolean renew) throws CertificateException {
    try {
      SCMSecurityProtocolProtos.SCMGetCertResponseProto response;
      HddsProtos.NodeDetailsProto.Builder reconDetailsProtoBuilder =
          HddsProtos.NodeDetailsProto.newBuilder()
              .setHostName(InetAddress.getLocalHost().getHostName())
              .setClusterId(clusterID)
              .setUuid(reconID)
              .setNodeType(HddsProtos.NodeType.RECON);
      // TODO: For SCM CA we should fetch certificate from multiple SCMs.
      response = getScmSecureClient().getCertificateChain(
          reconDetailsProtoBuilder.build(), getEncodedString(csr));

      // Persist certificates.
      if (response.hasX509CACertificate()) {
        String pemEncodedCert = response.getX509Certificate();
        CertificateCodec certCodec = new CertificateCodec(
            getSecurityConfig(), certificatePath);
        storeCertificate(pemEncodedCert, CAType.NONE, certCodec, false, !renew);
        storeCertificate(response.getX509CACertificate(),
            CAType.SUBORDINATE, certCodec, false, !renew);

        // Store Root CA certificate.
        if (response.hasX509RootCACertificate()) {
          storeCertificate(response.getX509RootCACertificate(),
              CAType.ROOT, certCodec, false, !renew);
        }
        return getX509Certificate(pemEncodedCert).getSerialNumber().toString();
      } else {
        throw new CertificateException("Unable to retrieve recon certificate " +
            "chain");
      }
    } catch (IOException | java.security.cert.CertificateException e) {
      LOG.error("Error while signing and storing SCM signed certificate.", e);
      throw new CertificateException(
          "Error while signing and storing SCM signed certificate.", e);
    }
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }
}
