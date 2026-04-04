package sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import support.SyncProviderFixture;
import support.SyncProviderFixtureArgumentsProvider;
import support.SyncTestContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SyncClientImplTest {
    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void getProviderExposesUbsaProvider(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            assertEquals(context.provider(), context.client().getProvider());
            assertTrue(Set.of(Provider.AWS, Provider.Azure, Provider.GCP).contains(context.client().getProvider()));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void createBucketThrowsWhenBucketAlreadyExists(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("duplicate");

            UbsaException error = assertThrows(
                    UbsaException.class,
                    () -> context.client().createBucket(Bucket.builder().name(bucketName).build())
            );

            assertEquals(HttpURLConnection.HTTP_CONFLICT, error.getStatusCode());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void createBlobUsingBlobRoundTripsThroughUbsaClient(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("blob");
            byte[] payload = "hello from ubsa".getBytes(StandardCharsets.UTF_8);
            Map<String, String> metadata = Map.of("owner", "ubsa", "case", "blob");
            String blobKey = "roundtrip/blob.txt";

            Blob blob = Blob.builder()
                    .bucket(bucketName)
                    .key(blobKey)
                    .content(payload)
                    .encoding("gzip")
                    .userMetadata(metadata)
                    .build();

            String etag = context.client().createBlob(bucketName, blob);

            assertTrue(context.client().blobExists(bucketName, blobKey));

            Blob loaded = context.client().getBlob(bucketName, blobKey);
            assertArrayEquals(payload, loaded.getContent());
            assertEquals(blobKey, loaded.getKey());
            assertEquals(bucketName, loaded.getBucket());
            assertEquals(payload.length, loaded.getSize());
            assertEquals("gzip", loaded.encoding());
            assertEquals(etag, loaded.getEtag());
            assertEquals(metadata, loaded.getUserMetadata());
            assertNotNull(loaded.getPublicURI());
            assertTrue(loaded.getPublicURI().toString().contains(bucketName));
            assertTrue(loaded.getPublicURI().toString().contains(blobKey));
            assertNotNull(loaded.lastModified());

            Blob metadataOnly = context.client().getBlobMetadata(bucketName, blobKey);
            assertNull(metadataOnly.getContent());
            assertEquals(payload.length, metadataOnly.getSize());
            assertEquals("gzip", metadataOnly.encoding());
            assertEquals(etag, metadataOnly.getEtag());
            assertEquals(metadata, metadataOnly.getUserMetadata());
            assertEquals(bucketName, metadataOnly.getBucket());
            assertEquals(blobKey, metadataOnly.getKey());
            assertNotNull(metadataOnly.getPublicURI());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void createBlobWithNullContentCreatesEmptyBlob(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("empty");
            String blobKey = "empty.bin";

            context.client().createBlob(bucketName, Blob.builder().bucket(bucketName).key(blobKey).content(null).build());

            Blob blob = context.client().getBlob(bucketName, blobKey);
            assertEquals(0L, blob.getSize());
            assertArrayEquals(new byte[0], blob.getContent());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void openBlobStreamAndByteRangeReturnExpectedContentAndValidateRanges(SyncProviderFixture fixture) throws IOException {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("stream");
            String blobKey = "payload.bin";
            byte[] payload = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
            context.client().createBlob(bucketName, Blob.builder().bucket(bucketName).key(blobKey).content(payload).build());

            try (InputStream stream = context.client().openBlobStream(bucketName, blobKey)) {
                assertArrayEquals(payload, stream.readAllBytes());
            }

            assertArrayEquals("34567".getBytes(StandardCharsets.UTF_8), context.client().getByteRange(bucketName, blobKey, 3, 7));

            assertThrows(IllegalArgumentException.class, () -> context.client().getByteRange(bucketName, blobKey, -1, 2));
            assertThrows(IllegalArgumentException.class, () -> context.client().getByteRange(bucketName, blobKey, 8, 7));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void fileUploadsSupportPlainAndConfiguredWrites(SyncProviderFixture fixture, @TempDir Path tempDir) throws IOException {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("file");

            Path plainFile = tempDir.resolve("plain.txt");
            byte[] plainPayload = "plain file upload".getBytes(StandardCharsets.UTF_8);
            Files.write(plainFile, plainPayload);
            String plainEtag = context.client().createBlob(bucketName, "plain.txt", plainFile);

            Blob plainBlob = context.client().getBlob(bucketName, "plain.txt");
            assertEquals(plainEtag, plainBlob.getEtag());
            assertArrayEquals(plainPayload, plainBlob.getContent());
            assertEquals(plainPayload.length, plainBlob.getSize());

            Path configuredFile = tempDir.resolve("configured.txt");
            byte[] configuredPayload = "configured file upload".getBytes(StandardCharsets.UTF_8);
            Files.write(configuredFile, configuredPayload);
            BlobWriteOptions options = BlobWriteOptions.builder()
                    .encoding("br")
                    .userMetadata(Map.of("source", "file", "mode", "configured"))
                    .build();

            String configuredEtag = context.client().createBlob(bucketName, "configured.txt", configuredFile, options);

            Blob configuredBlob = context.client().getBlob(bucketName, "configured.txt");
            assertEquals(configuredEtag, configuredBlob.getEtag());
            assertArrayEquals(configuredPayload, configuredBlob.getContent());
            assertEquals("br", configuredBlob.encoding());
            assertEquals(options.userMetadata(), configuredBlob.getUserMetadata());

            assertThrows(IllegalArgumentException.class, () -> context.client().createBlob(bucketName, "missing.txt", tempDir.resolve("missing.txt")));
            assertThrows(IllegalArgumentException.class, () -> context.client().createBlob(bucketName, "null.txt", null));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void streamUploadsSupportMetadataAndValidateArguments(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("streamupload");
            byte[] payload = "streamed upload body".getBytes(StandardCharsets.UTF_8);
            BlobWriteOptions options = BlobWriteOptions.builder()
                    .encoding("deflate")
                    .userMetadata(Map.of("source", "stream"))
                    .build();

            String etag = context.client().createBlob(
                    bucketName,
                    "stream.txt",
                    new ByteArrayInputStream(payload),
                    payload.length,
                    options
            );

            Blob blob = context.client().getBlob(bucketName, "stream.txt");
            assertEquals(etag, blob.getEtag());
            assertArrayEquals(payload, blob.getContent());
            assertEquals("deflate", blob.encoding());
            assertEquals(options.userMetadata(), blob.getUserMetadata());

            assertThrows(
                    IllegalArgumentException.class,
                    () -> context.client().createBlob(bucketName, "null-stream.txt", null, 0, options)
            );
            assertThrows(
                    IllegalArgumentException.class,
                    () -> context.client().createBlob(bucketName, "negative.txt", new ByteArrayInputStream(payload), -1, options)
            );
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void streamUploadsRejectMismatchedContentLength(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("streammismatch");
            byte[] payload = "stream mismatch".getBytes(StandardCharsets.UTF_8);

            UbsaException shortStream = assertThrows(
                    UbsaException.class,
                    () -> context.client().createBlob(bucketName, "short.txt", new ByteArrayInputStream(payload), payload.length + 1L, null)
            );
            assertFalse(context.client().blobExists(bucketName, "short.txt"));
            assertNotNull(shortStream.getCause());

            UbsaException longStream = assertThrows(
                    UbsaException.class,
                    () -> context.client().createBlob(bucketName, "long.txt", new ByteArrayInputStream(payload), payload.length - 1L, null)
            );
            assertFalse(context.client().blobExists(bucketName, "long.txt"));
            assertNotNull(longStream.getCause());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void deleteBlobIfExistsAndCopyBlobMatchUbsaState(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String sourceBucket = context.createBucket("copy-src");
            String destinationBucket = context.createBucket("copy-dst");
            String sourceBlobKey = "source.txt";
            String destinationBlobKey = "copied.txt";
            byte[] payload = "copy me".getBytes(StandardCharsets.UTF_8);
            Blob sourceBlob = Blob.builder()
                    .bucket(sourceBucket)
                    .key(sourceBlobKey)
                    .content(payload)
                    .encoding("gzip")
                    .userMetadata(Map.of("source", "ubsa"))
                    .build();
            context.client().createBlob(sourceBucket, sourceBlob);

            String copiedEtag = context.client().copyBlob(sourceBucket, sourceBlobKey, destinationBucket, destinationBlobKey);

            Blob destinationBlob = context.client().getBlob(destinationBucket, destinationBlobKey);
            assertEquals(copiedEtag, destinationBlob.getEtag());
            assertArrayEquals(payload, destinationBlob.getContent());
            assertEquals("gzip", destinationBlob.encoding());
            assertEquals(Map.of("source", "ubsa"), destinationBlob.getUserMetadata());

            context.client().deleteBlobIfExists(destinationBucket, destinationBlobKey);
            assertFalse(context.client().blobExists(destinationBucket, destinationBlobKey));
            assertDoesNotThrow(() -> context.client().deleteBlobIfExists(destinationBucket, destinationBlobKey));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void listBlobsAndIterateBlobsMatchUbsaListingWithPagination(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("listblobs");
            createTextBlob(context, bucketName, "prefix/a.txt", "A", Map.of("listed", "true"), null);
            createTextBlob(context, bucketName, "prefix/b.txt", "BB", Map.of("listed", "true"), null);
            createTextBlob(context, bucketName, "prefix/c.txt", "CCC", Map.of("listed", "true"), null);
            createTextBlob(context, bucketName, "other/z.txt", "ignored", Map.of("listed", "true"), null);

            ListingPage<Blob> firstPage = context.client().listBlobs(bucketName, "prefix/", PageRequest.builder().pageSize(2).build());
            assertEquals(2, firstPage.getItems().size());
            assertTrue(firstPage.hasNextPage());
            firstPage.getItems().forEach(blob -> assertListedBlob(bucketName, blob));

            ListingPage<Blob> secondPage = context.client().listBlobs(
                    bucketName,
                    "prefix/",
                    PageRequest.builder().pageSize(2).continuationToken(firstPage.getNextContinuationToken()).build()
            );
            assertEquals(1, secondPage.getItems().size());
            assertFalse(secondPage.hasNextPage());
            secondPage.getItems().forEach(blob -> assertListedBlob(bucketName, blob));

            Set<String> listedKeys = new HashSet<>();
            firstPage.getItems().forEach(blob -> listedKeys.add(blob.getKey()));
            secondPage.getItems().forEach(blob -> listedKeys.add(blob.getKey()));
            assertEquals(Set.of("prefix/a.txt", "prefix/b.txt", "prefix/c.txt"), listedKeys);

            ListingPage<Blob> emptyPage = context.client().listBlobs(bucketName, "missing/", null);
            assertTrue(emptyPage.getItems().isEmpty());
            assertFalse(emptyPage.hasNextPage());
            assertNull(emptyPage.getNextContinuationToken());

            Set<String> iteratedKeys = new HashSet<>();
            context.client().iterateBlobs(bucketName, "prefix/", 1).forEach(blob -> iteratedKeys.add(blob.getKey()));
            assertEquals(Set.of("prefix/a.txt", "prefix/b.txt", "prefix/c.txt"), iteratedKeys);
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void listBucketsListAllBucketsAndIterateBucketsExposeCreatedBuckets(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String prefix = context.newBucketName("listbucket").substring(0, 12);
            String first = context.createBucket(prefix);
            String second = context.createBucket(prefix);
            String third = context.createBucket(prefix);
            Set<String> expectedNames = Set.of(first, second, third);

            PageRequest request = PageRequest.builder().pageSize(1).build();
            Set<String> pagedNames = new HashSet<>();
            while (true) {
                ListingPage<Bucket> page = context.client().listBuckets(request);
                page.getItems().stream()
                        .filter(bucket -> expectedNames.contains(bucket.getName()))
                        .forEach(bucket -> {
                            pagedNames.add(bucket.getName());
                            assertNotNull(bucket.getPublicURI());
                        });
                if (!page.hasNextPage()) {
                    break;
                }
                request = PageRequest.builder().pageSize(1).continuationToken(page.getNextContinuationToken()).build();
            }
            assertEquals(expectedNames, pagedNames);

            Set<String> allBucketNames = new HashSet<>();
            context.client().listAllBuckets().stream()
                    .map(Bucket::getName)
                    .filter(expectedNames::contains)
                    .forEach(allBucketNames::add);
            assertEquals(expectedNames, allBucketNames);

            Set<String> iteratedBucketNames = new HashSet<>();
            context.client().iterateBuckets(1).forEach(bucket -> {
                if (expectedNames.contains(bucket.getName())) {
                    iteratedBucketNames.add(bucket.getName());
                }
            });
            assertEquals(expectedNames, iteratedBucketNames);
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void signedUrlsAllowDownloadingAndUploading(SyncProviderFixture fixture) throws Exception {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("signed");
            String getBlobKey = "download.txt";
            byte[] downloadPayload = "download through signed url".getBytes(StandardCharsets.UTF_8);
            createTextBlob(context, bucketName, getBlobKey, new String(downloadPayload, StandardCharsets.UTF_8), null, null);

            URL getUrl = context.client().generateGetUrl(bucketName, getBlobKey, Duration.ofMinutes(5));
            assertArrayEquals(downloadPayload, context.readRequired(getUrl));

            byte[] uploadPayload = "uploaded through signed url".getBytes(StandardCharsets.UTF_8);
            URL putUrl = context.client().generatePutUrl(bucketName, "upload.txt", Duration.ofMinutes(5), "text/plain");
            context.writeRequired(putUrl, "text/plain", uploadPayload);

            Blob uploadedBlob = context.client().getBlob(bucketName, "upload.txt");
            assertArrayEquals(uploadPayload, uploadedBlob.getContent());

            assertThrows(IllegalArgumentException.class, () -> context.client().generateGetUrl(bucketName, getBlobKey, Duration.ZERO));
            assertThrows(IllegalArgumentException.class, () -> context.client().generatePutUrl(bucketName, "bad.txt", Duration.ofSeconds(-1), "text/plain"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void signedUrlsAgainstMissingBucketReturnNotFound(SyncProviderFixture fixture) throws Exception {
        try (SyncTestContext context = fixture.openContext()) {
            String missingBucket = context.newBucketName("missing-signed");

            URL getUrl = context.client().generateGetUrl(missingBucket, "missing.txt", Duration.ofMinutes(5));
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, context.getSignedUrlResponse(getUrl).statusCode());

            URL putUrl = context.client().generatePutUrl(missingBucket, "missing.txt", Duration.ofMinutes(5), "text/plain");
            assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    context.putSignedUrlResponse(putUrl, "text/plain", "missing bucket".getBytes(StandardCharsets.UTF_8)).statusCode()
            );
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void missingBlobOperationsRaiseProviderNeutralExceptions(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("missingblob");
            String missingKey = "missing.txt";

            UbsaException getBlob = assertThrows(UbsaException.class, () -> context.client().getBlob(bucketName, missingKey));
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getBlob.getStatusCode());

            UbsaException metadata = assertThrows(UbsaException.class, () -> context.client().getBlobMetadata(bucketName, missingKey));
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, metadata.getStatusCode());

            UbsaException range = assertThrows(
                    UbsaException.class,
                    () -> context.client().getByteRange(bucketName, missingKey, 0, 2)
            );
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, range.getStatusCode());

            UbsaException stream = assertThrows(UbsaException.class, () -> {
                try (InputStream ignored = context.client().openBlobStream(bucketName, missingKey)) {
                    ignored.readAllBytes();
                }
            });
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, stream.getStatusCode());
        }
    }

    private static void createTextBlob(
            SyncTestContext context,
            String bucketName,
            String blobKey,
            String content,
            Map<String, String> metadata,
            String encoding
    ) {
        Blob.Builder builder = Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .content(content.getBytes(StandardCharsets.UTF_8));
        if (metadata != null) {
            builder.userMetadata(metadata);
        }
        if (encoding != null) {
            builder.encoding(encoding);
        }
        context.client().createBlob(bucketName, builder.build());
    }

    private static void assertListedBlob(String bucketName, Blob blob) {
        assertEquals(bucketName, blob.getBucket());
        assertTrue(blob.getKey().startsWith("prefix/"));
        assertTrue(blob.getSize() >= 0);
        assertNull(blob.getContent());
        assertNotNull(blob.getPublicURI());
    }
}
