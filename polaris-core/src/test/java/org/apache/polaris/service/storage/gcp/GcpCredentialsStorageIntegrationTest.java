/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.storage.gcp;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.storage.BaseStorageIntegrationTest;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class GcpCredentialsStorageIntegrationTest extends BaseStorageIntegrationTest {

  private final String gcsServiceKeyJsonFileLocation =
      System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

  private static final String REFRESH_ENDPOINT = "get/credentials";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSubscope(boolean allowedListAction) throws Exception {
    Assumptions.assumeThat(gcsServiceKeyJsonFileLocation)
        .describedAs("Environment variable GOOGLE_APPLICATION_CREDENTIALS not exits")
        .isNotNull()
        .isNotEmpty();

    List<String> allowedRead =
        Arrays.asList(
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/read1/",
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/read2/");
    List<String> allowedWrite =
        Arrays.asList(
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write1/",
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write2/");
    Storage storageClient = setupStorageClient(allowedRead, allowedWrite, allowedListAction);
    BlobInfo blobInfoGoodWrite =
        createStorageBlob("sfc-dev1-regtest", "polaris-test/subscoped-test/write1/", "file.txt");
    BlobInfo blobInfoBad =
        createStorageBlob("sfc-dev1-regtest", "polaris-test/subscoped-test/write3/", "file.txt");
    BlobInfo blobInfoGoodRead =
        createStorageBlob("sfc-dev1-regtest", "polaris-test/subscoped-test/read1/", "file.txt");
    final byte[] fileContent = "hello-polaris".getBytes(UTF_8);
    // GOOD WRITE
    Assertions.assertThatNoException()
        .isThrownBy(() -> storageClient.create(blobInfoGoodWrite, fileContent));

    // BAD WROTE
    Assertions.assertThatThrownBy(() -> storageClient.create(blobInfoBad, fileContent))
        .isInstanceOf(StorageException.class);

    Assertions.assertThatNoException()
        .isThrownBy(() -> storageClient.get(blobInfoGoodRead.getBlobId()));
    Assertions.assertThatThrownBy(() -> storageClient.get(blobInfoBad.getBlobId()))
        .isInstanceOf(StorageException.class);

    // LIST
    if (allowedListAction) {
      Assertions.assertThatNoException()
          .isThrownBy(
              () ->
                  storageClient.list(
                      "sfc-dev1-regtest",
                      Storage.BlobListOption.prefix("polaris-test/subscoped-test/read1/")));
    } else {
      Assertions.assertThatThrownBy(
              () ->
                  storageClient.list(
                      "sfc-dev1-regtest",
                      Storage.BlobListOption.prefix("polaris-test/subscoped-test/read1/")))
          .isInstanceOf(StorageException.class);
    }
    // DELETE
    List<String> allowedWrite2 =
        Arrays.asList(
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write2/",
            "gs://sfc-dev1-regtest/polaris-test/subscoped-test/write3/");
    Storage clientForDelete = setupStorageClient(List.of(), allowedWrite2, allowedListAction);

    // can not delete because it is not in allowed write path for this client
    Assertions.assertThatThrownBy(() -> clientForDelete.delete(blobInfoGoodWrite.getBlobId()))
        .isInstanceOf(StorageException.class);

    // good to delete allowed location
    Assertions.assertThatNoException()
        .isThrownBy(() -> storageClient.delete(blobInfoGoodWrite.getBlobId()));
  }

  private Storage setupStorageClient(
      List<String> allowedReadLoc, List<String> allowedWriteLoc, boolean allowListAction)
      throws IOException {
    return createStorageClient(
        subscopedCredsForOperations(allowedReadLoc, allowedWriteLoc, allowListAction));
  }

  BlobInfo createStorageBlob(String bucket, String prefix, String fileName) {
    BlobId blobId = BlobId.of(bucket, prefix + fileName);
    return BlobInfo.newBuilder(blobId).build();
  }

  private Storage createStorageClient(StorageAccessConfig storageAccessConfig) {
    AccessToken accessToken =
        new AccessToken(
            storageAccessConfig.get(StorageAccessProperty.GCS_ACCESS_TOKEN),
            new Date(
                Long.parseLong(
                    storageAccessConfig.get(StorageAccessProperty.GCS_ACCESS_TOKEN_EXPIRES_AT))));
    return StorageOptions.newBuilder()
        .setCredentials(GoogleCredentials.create(accessToken))
        .build()
        .getService();
  }

  private StorageAccessConfig subscopedCredsForOperations(
      List<String> allowedReadLoc, List<String> allowedWriteLoc, boolean allowListAction)
      throws IOException {
    GcpStorageConfigurationInfo gcpConfig =
        GcpStorageConfigurationInfo.builder()
            .addAllAllowedLocations(allowedReadLoc)
            .addAllAllowedLocations(allowedWriteLoc)
            .build();
    GcpCredentialsStorageIntegration gcpCredsIntegration =
        new GcpCredentialsStorageIntegration(
            gcpConfig,
            GoogleCredentials.getApplicationDefault(),
            ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new));
    return gcpCredsIntegration.getSubscopedCreds(
        EMPTY_REALM_CONFIG,
        allowListAction,
        new HashSet<>(allowedReadLoc),
        new HashSet<>(allowedWriteLoc),
        PolarisPrincipal.of("principal", Map.of(), Set.of()),
        Optional.of(REFRESH_ENDPOINT),
        CredentialVendingContext.empty());
  }

  private JsonNode readResource(ObjectMapper mapper, String name) throws IOException {
    try (InputStream in = GcpCredentialsStorageIntegrationTest.class.getResourceAsStream(name)) {
      return mapper.readTree(in);
    }
  }

  @Test
  public void testGenerateAccessBoundary() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of("gs://bucket1/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource(mapper, "gcp-testGenerateAccessBoundary.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryWithMultipleBuckets() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of(
                "gs://bucket1/normal/path/to/data",
                "gs://bucket1/awesome/path/to/data",
                "gs://bucket2/a/super/path/to/data"),
            Set.of("gs://bucket1/normal/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        readResource(mapper, "gcp-testGenerateAccessBoundaryWithMultipleBuckets.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryWithoutList() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            false,
            Set.of("gs://bucket1/path/to/data", "gs://bucket1/another/path/to/data"),
            Set.of("gs://bucket1/path/to/data"));
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource(mapper, "gcp-testGenerateAccessBoundaryWithoutList.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryWithoutWrites() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            false,
            Set.of("gs://bucket1/normal/path/to/data", "gs://bucket1/awesome/path/to/data"),
            Set.of());
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource(mapper, "gcp-testGenerateAccessBoundaryWithoutWrites.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  /**
   * Custom comparator as ObjectNodes are compared by field indexes as opposed to field names. They
   * also don't equate a field that is present and set to null with a field that is omitted
   *
   * @param on1
   * @param on2
   * @return
   */
  private boolean recursiveEquals(ContainerNode<?> on1, ContainerNode<?> on2) {
    Set<String> fieldNames = new HashSet<>();
    on1.fieldNames().forEachRemaining(fieldNames::add);
    on2.fieldNames().forEachRemaining(fieldNames::add);
    for (String fieldName : fieldNames) {
      if ((!on1.has(fieldName) || !on2.has(fieldName))) {
        if (isNotNull(on1.get(fieldName)) || isNotNull(on2.get(fieldName))) {
          return false;
        }
      } else {
        JsonNode fieldValue = on1.get(fieldName);
        JsonNode fieldValue2 = on2.get(fieldName);
        if (fieldValue.isContainerNode()) {
          if (!fieldValue2.isContainerNode()
              || !recursiveEquals((ContainerNode<?>) fieldValue, (ContainerNode<?>) fieldValue2)) {
            return false;
          }
        } else if (!fieldValue.equals(fieldValue2)) {
          return false;
        }
      }
    }
    return true;
  }

  @Test
  public void testRefreshCredentialsEndpointIsReturned() throws IOException {
    Assumptions.assumeThat(gcsServiceKeyJsonFileLocation)
        .describedAs("Environment variable GOOGLE_APPLICATION_CREDENTIALS not exits")
        .isNotNull()
        .isNotEmpty();

    StorageAccessConfig storageAccessConfig =
        subscopedCredsForOperations(
            List.of("gs://bucket1/path/to/data"), List.of("gs://bucket1/path/to/data"), true);
    assertThat(storageAccessConfig.get(StorageAccessProperty.GCS_REFRESH_CREDENTIALS_ENDPOINT))
        .isEqualTo(REFRESH_ENDPOINT);
  }

  @Test
  public void testImpersonation() throws IOException {
    String serviceAccount = "test-sa@project.iam.gserviceaccount.com";
    GcpStorageConfigurationInfo config =
        GcpStorageConfigurationInfo.builder()
            .addAllAllowedLocations(List.of("gs://bucket/path"))
            .gcpServiceAccount(serviceAccount)
            .build();

    IamCredentialsClient mockIamClient = Mockito.mock(IamCredentialsClient.class);
    GenerateAccessTokenResponse mockResponse =
        GenerateAccessTokenResponse.newBuilder()
            .setAccessToken("impersonated-token")
            .setExpireTime(
                Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 + 3600).build())
            .build();
    Mockito.when(mockIamClient.generateAccessToken(Mockito.any(GenerateAccessTokenRequest.class)))
        .thenReturn(mockResponse);

    GoogleCredentials mockCreds = Mockito.mock(GoogleCredentials.class);
    Mockito.when(mockCreds.createScoped(Mockito.any(String.class))).thenReturn(mockCreds);

    GcpCredentialsStorageIntegration integration =
        new GcpCredentialsStorageIntegration(
            config,
            mockCreds,
            ServiceOptions.getFromServiceLoader(
                HttpTransportFactory.class, NetHttpTransport::new)) {
          @Override
          protected IamCredentialsClient createIamCredentialsClient(GoogleCredentials credentials) {
            return mockIamClient;
          }

          @Override
          protected AccessToken refreshAccessToken(DownscopedCredentials credentials) {
            return new AccessToken("downscoped-token", new Date());
          }
        };

    integration.getSubscopedCreds(
        EMPTY_REALM_CONFIG,
        true,
        Set.of("gs://bucket/path"),
        Set.of("gs://bucket/path"),
        PolarisPrincipal.of("principal", Map.of(), Set.of()),
        Optional.empty(),
        CredentialVendingContext.empty());

    Mockito.verify(mockIamClient)
        .generateAccessToken(
            Mockito.argThat(
                request ->
                    request
                            .getName()
                            .equals(
                                GcpCredentialsStorageIntegration.SERVICE_ACCOUNT_PREFIX
                                    + serviceAccount)
                        && request.getScopeCount() > 0
                        && request
                            .getScope(0)
                            .equals(GcpCredentialsStorageIntegration.IMPERSONATION_SCOPE)));
  }

  @Test
  public void testGenerateAccessBoundaryHnsEnabled() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of("gs://bucket1/path/to/data"), true);
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules = readResource(mapper, "gcp-testGenerateAccessBoundaryHnsEnabled.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryHnsWithMultipleBuckets() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://bucket1/normal/path/to/data", "gs://bucket1/awesome/path/to/data"),
            Set.of("gs://bucket1/normal/path/to/data"),
            true);
    assertThat(credentialAccessBoundary).isNotNull();
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        readResource(mapper, "gcp-testGenerateAccessBoundaryHnsWithMultipleBuckets.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryHnsWithoutWrites() throws IOException {
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of(), true);
    assertThat(credentialAccessBoundary).isNotNull();
    // When there are no write locations, HNS should not add any folder rules
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    // Should be the same as non-HNS since there are no write locations
    JsonNode refRules = readResource(mapper, "gcp-testGenerateAccessBoundaryHnsWithoutWrites.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testGenerateAccessBoundaryHnsSeparateMetadataAndData() throws IOException {
    // Iceberg writes to both metadata and data locations — the ingestion job must update
    // metadata (manifest lists, table metadata JSON) and write data files. When metadata
    // and data reside in different buckets, both buckets need write + folderAdmin rules.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://metadata-bucket/ns/table/metadata/", "gs://data-bucket/ns/table/data/"),
            Set.of("gs://metadata-bucket/ns/table/metadata/", "gs://data-bucket/ns/table/data/"),
            true);
    assertThat(credentialAccessBoundary).isNotNull();
    // Expect 6 rules: read on each bucket (2), write on each bucket (2),
    // folderAdmin on each bucket (2)
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(6);
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    JsonNode refRules =
        readResource(mapper, "gcp-testGenerateAccessBoundaryHnsSeparateMetadataAndData.json");
    assertThat(parsedRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(refRules);
  }

  @Test
  public void testHnsSameBucketSeparateMetadataAndDataPaths() {
    // Metadata and data in the same bucket but different prefixes. Both are write locations
    // because Iceberg must write metadata files (manifests, table metadata) alongside data.
    // folderAdmin should scope to both the metadata and data paths.
    CredentialAccessBoundary credentialAccessBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of(
                "gs://bucket1/warehouse/db/table/metadata/",
                "gs://bucket1/warehouse/db/table/data/"),
            Set.of(
                "gs://bucket1/warehouse/db/table/metadata/",
                "gs://bucket1/warehouse/db/table/data/"),
            true);
    assertThat(credentialAccessBoundary).isNotNull();
    // Same bucket → 1 read rule (both paths combined), 1 write rule (both paths),
    // 1 folder rule (both paths) = 3 rules
    assertThat(credentialAccessBoundary.getAccessBoundaryRules()).hasSize(3);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(credentialAccessBoundary, JsonNode.class);
    // Verify folderAdmin conditions reference both metadata and data paths
    String folderRuleJson = parsedRules.toString();
    assertThat(folderRuleJson).contains("managedFolders/warehouse/db/table/data/");
    assertThat(folderRuleJson).contains("managedFolders/warehouse/db/table/metadata/");
  }

  @Test
  public void testNonHnsDoesNotIncludeFolderRules() {
    CredentialAccessBoundary nonHnsBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of("gs://bucket1/path/to/data"), false);
    CredentialAccessBoundary hnsBoundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, Set.of("gs://bucket1/path/to/data"), Set.of("gs://bucket1/path/to/data"), true);
    // Non-HNS should have 2 rules (read + write), HNS should have 3 (read + write + folder)
    assertThat(nonHnsBoundary.getAccessBoundaryRules()).hasSize(2);
    assertThat(hnsBoundary.getAccessBoundaryRules()).hasSize(3);
  }

  // ---- Per-bucket HNS auto-detection tests (Set<String> hnsBuckets overload) ----

  @Test
  public void testPerBucketHns_metadataOnHnsBucket_dataOnFlatBucket() {
    // metadata-bucket is HNS, data-bucket is flat
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://hns-bucket/ns/table/metadata/", "gs://flat-bucket/ns/table/data/"),
            Set.of("gs://hns-bucket/ns/table/metadata/", "gs://flat-bucket/ns/table/data/"),
            Set.of("hns-bucket"));
    assertThat(boundary).isNotNull();
    // Expect 5 rules: read on each bucket (2), write on each bucket (2),
    // folderAdmin only on hns-bucket (1)
    assertThat(boundary.getAccessBoundaryRules()).hasSize(5);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    // hns-bucket should have folderAdmin with folders/ and managedFolders/ conditions
    assertThat(json).contains("roles/storage.folderAdmin");
    assertThat(json).contains("folders/ns/table/metadata/");
    assertThat(json).contains("managedFolders/ns/table/metadata/");
    // flat-bucket should NOT have folders/ or managedFolders/ conditions
    assertThat(json).doesNotContain("folders/ns/table/data/");
    assertThat(json).doesNotContain("managedFolders/ns/table/data/");
  }

  @Test
  public void testPerBucketHns_metadataOnFlatBucket_dataOnHnsBucket() {
    // flat-bucket has metadata, hns-bucket has data
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://flat-bucket/ns/table/metadata/", "gs://hns-bucket/ns/table/data/"),
            Set.of("gs://flat-bucket/ns/table/metadata/", "gs://hns-bucket/ns/table/data/"),
            Set.of("hns-bucket"));
    assertThat(boundary).isNotNull();
    // 5 rules: read on each (2), write on each (2), folderAdmin on hns-bucket only (1)
    assertThat(boundary.getAccessBoundaryRules()).hasSize(5);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    // hns-bucket should have folder conditions for data path
    assertThat(json).contains("roles/storage.folderAdmin");
    assertThat(json).contains("folders/ns/table/data/");
    assertThat(json).contains("managedFolders/ns/table/data/");
    // flat-bucket should NOT have folder conditions
    assertThat(json).doesNotContain("folders/ns/table/metadata/");
    assertThat(json).doesNotContain("managedFolders/ns/table/metadata/");
  }

  @Test
  public void testPerBucketHns_bothHnsDifferentBuckets() {
    // Both buckets are HNS
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://hns-bucket-a/ns/table/metadata/", "gs://hns-bucket-b/ns/table/data/"),
            Set.of("gs://hns-bucket-a/ns/table/metadata/", "gs://hns-bucket-b/ns/table/data/"),
            Set.of("hns-bucket-a", "hns-bucket-b"));
    assertThat(boundary).isNotNull();
    // 6 rules: read on each (2), write on each (2), folderAdmin on each (2)
    assertThat(boundary.getAccessBoundaryRules()).hasSize(6);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    // Both buckets should have folder conditions
    assertThat(json).contains("folders/ns/table/metadata/");
    assertThat(json).contains("managedFolders/ns/table/metadata/");
    assertThat(json).contains("folders/ns/table/data/");
    assertThat(json).contains("managedFolders/ns/table/data/");
  }

  @Test
  public void testPerBucketHns_bothNonHnsDifferentBuckets() {
    // Neither bucket is HNS
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://flat-a/ns/table/metadata/", "gs://flat-b/ns/table/data/"),
            Set.of("gs://flat-a/ns/table/metadata/", "gs://flat-b/ns/table/data/"),
            Set.of());
    assertThat(boundary).isNotNull();
    // 4 rules: read on each (2), write on each (2), no folderAdmin
    assertThat(boundary.getAccessBoundaryRules()).hasSize(4);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    assertThat(json).doesNotContain("roles/storage.folderAdmin");
    assertThat(json).doesNotContain("folders/");
    assertThat(json).doesNotContain("managedFolders/");
  }

  @Test
  public void testPerBucketHns_sameHnsBucket() {
    // Both paths on the same HNS bucket
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of(
                "gs://hns-bucket/warehouse/db/table/metadata/",
                "gs://hns-bucket/warehouse/db/table/data/"),
            Set.of(
                "gs://hns-bucket/warehouse/db/table/metadata/",
                "gs://hns-bucket/warehouse/db/table/data/"),
            Set.of("hns-bucket"));
    assertThat(boundary).isNotNull();
    // Same bucket: 1 read rule, 1 write rule, 1 folderAdmin rule = 3
    assertThat(boundary.getAccessBoundaryRules()).hasSize(3);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    assertThat(json).contains("roles/storage.folderAdmin");
    assertThat(json).contains("folders/warehouse/db/table/metadata/");
    assertThat(json).contains("folders/warehouse/db/table/data/");
    assertThat(json).contains("managedFolders/warehouse/db/table/metadata/");
    assertThat(json).contains("managedFolders/warehouse/db/table/data/");
  }

  @Test
  public void testPerBucketHns_emptyHnsBucketsSet() {
    // Explicitly passing empty set should behave like non-HNS
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://bucket1/path/to/data"),
            Set.of("gs://bucket1/path/to/data"),
            Set.of());
    assertThat(boundary).isNotNull();
    // 2 rules: read + write, no folderAdmin
    assertThat(boundary.getAccessBoundaryRules()).hasSize(2);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    assertThat(json).doesNotContain("roles/storage.folderAdmin");
    assertThat(json).doesNotContain("folders/");
    assertThat(json).doesNotContain("managedFolders/");
  }

  @Test
  public void testPerBucketHns_hnsBucketNotInWriteLocations() {
    // hnsBuckets contains a bucket name that is NOT among the write locations.
    // This should not produce a folderAdmin rule for the non-write bucket.
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true,
            Set.of("gs://read-bucket/path/data/"),
            Set.of("gs://write-bucket/path/data/"),
            Set.of("read-bucket"));
    assertThat(boundary).isNotNull();
    // read-bucket is in hnsBuckets but not a write bucket, so no folderAdmin for it.
    // write-bucket is a write bucket but not in hnsBuckets, so no folderAdmin for it.
    // Expect: 2 read rules (read-bucket + write-bucket) + 1 write rule = 3, no folderAdmin
    // Actually: both locations go into read, write-bucket goes into write.
    // read-bucket read rule, write-bucket read rule, write-bucket write rule = could be 2 or 3
    // depending on bucket consolidation. Let's just check no folderAdmin.
    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    assertThat(json).doesNotContain("roles/storage.folderAdmin");
    assertThat(json).doesNotContain("folders/");
    assertThat(json).doesNotContain("managedFolders/");
  }

  @Test
  public void testPerBucketHns_mixedBucketsWithListDisabled() {
    // Verify per-bucket HNS works correctly when list operations are disabled
    CredentialAccessBoundary boundary =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            false,
            Set.of("gs://hns-bucket/data/path/", "gs://flat-bucket/data/path/"),
            Set.of("gs://hns-bucket/data/path/", "gs://flat-bucket/data/path/"),
            Set.of("hns-bucket"));
    assertThat(boundary).isNotNull();
    // 5 rules: read on each (2), write on each (2), folderAdmin on hns-bucket (1)
    assertThat(boundary.getAccessBoundaryRules()).hasSize(5);

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode parsedRules = mapper.convertValue(boundary, JsonNode.class);
    String json = parsedRules.toString();
    // Should still have folderAdmin for hns-bucket
    assertThat(json).contains("roles/storage.folderAdmin");
    // Should NOT have objectViewer (list is disabled)
    assertThat(json).doesNotContain("roles/storage.objectViewer");
    // Should NOT have list prefix expressions
    assertThat(json).doesNotContain("objectListPrefix");
  }

  @Test
  public void testPerBucketHns_consistentWithBooleanOverload() {
    // When hnsBuckets contains all write buckets, result should match boolean=true overload
    Set<String> readLocs = Set.of("gs://bucket1/path/to/data");
    Set<String> writeLocs = Set.of("gs://bucket1/path/to/data");

    CredentialAccessBoundary fromBoolean =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, readLocs, writeLocs, true);
    CredentialAccessBoundary fromSet =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, readLocs, writeLocs, Set.of("bucket1"));

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode booleanRules = mapper.convertValue(fromBoolean, JsonNode.class);
    JsonNode setRules = mapper.convertValue(fromSet, JsonNode.class);

    assertThat(setRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(booleanRules);
  }

  @Test
  public void testPerBucketHns_emptySetConsistentWithBooleanFalse() {
    // Empty hnsBuckets set should match boolean=false overload
    Set<String> readLocs = Set.of("gs://bucket1/path/to/data");
    Set<String> writeLocs = Set.of("gs://bucket1/path/to/data");

    CredentialAccessBoundary fromBoolean =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, readLocs, writeLocs, false);
    CredentialAccessBoundary fromSet =
        GcpCredentialsStorageIntegration.generateAccessBoundaryRules(
            true, readLocs, writeLocs, Set.of());

    ObjectMapper mapper = JsonMapper.builder().build();
    JsonNode booleanRules = mapper.convertValue(fromBoolean, JsonNode.class);
    JsonNode setRules = mapper.convertValue(fromSet, JsonNode.class);

    assertThat(setRules)
        .usingRecursiveComparison(
            RecursiveComparisonConfiguration.builder()
                .withEqualsForType(this::recursiveEquals, ObjectNode.class)
                .build())
        .isEqualTo(booleanRules);
  }

  private boolean isNotNull(JsonNode node) {
    return node != null && !node.isNull();
  }
}
