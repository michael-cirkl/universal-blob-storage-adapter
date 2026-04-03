package sync;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import michaelcirkl.ubsa.client.exception.UbsaException;
import michaelcirkl.ubsa.client.exception.types.BlobNotFoundException;
import michaelcirkl.ubsa.client.exception.types.BucketAlreadyExistsException;
import michaelcirkl.ubsa.client.exception.types.BucketNotFoundException;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import support.AzureSampleClientSupport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AzureSyncClientImplTest {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    private BlobServiceClient nativeClient;
    private BlobStorageSyncClient client;
    private final List<String> createdContainers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        nativeClient = AzureSampleClientSupport.createNativeSyncClient();
        client = BlobStorageClientFactory.getSyncClient(nativeClient);
    }

    @AfterEach
    void tearDown() {
        for (String containerName : createdContainers) {
            BlobContainerClient containerClient = nativeClient.getBlobContainerClient(containerName);
            if (!containerClient.exists()) {
                continue;
            }
            containerClient.listBlobs().forEach(blobItem -> containerClient.getBlobClient(blobItem.getName()).deleteIfExists());
            containerClient.deleteIfExists();
        }
        createdContainers.clear();
    }

    @Test
    void getProviderAndUnwrapExposeAzureSdkClient() {
        assertEquals(Provider.Azure, client.getProvider());
        assertSame(nativeClient, client.unwrap(BlobServiceClient.class));
        assertNull(client.unwrap(String.class));

        assertThrows(IllegalArgumentException.class, () -> client.unwrap(null));
    }

    @Test
    void createBucketThrowsWhenContainerAlreadyExists() {
        String bucketName = createContainer("duplicate");

        BucketAlreadyExistsException error = assertThrows(
                BucketAlreadyExistsException.class,
                () -> client.createBucket(Bucket.builder().name(bucketName).build())
        );

        assertEquals(HttpURLConnection.HTTP_CONFLICT, error.getStatusCode());
    }

    @Test
    void createBlobUsingBlobRoundTripsThroughNativeClient() {
        String bucketName = createContainer("blob");
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

        String etag = client.createBlob(bucketName, blob);

        BlobClient nativeBlobClient = nativeClient.getBlobContainerClient(bucketName).getBlobClient(blobKey);
        BlobProperties properties = nativeBlobClient.getProperties();
        assertEquals(properties.getETag(), etag);
        assertArrayEquals(payload, nativeBlobClient.downloadContent().toBytes());
        assertEquals("gzip", properties.getContentEncoding());
        assertEquals(metadata, properties.getMetadata());

        assertTrue(client.blobExists(bucketName, blobKey));

        Blob loaded = client.getBlob(bucketName, blobKey);
        assertArrayEquals(payload, loaded.getContent());
        assertEquals(blobKey, loaded.getKey());
        assertEquals(bucketName, loaded.getBucket());
        assertEquals(payload.length, loaded.getSize());
        assertEquals("gzip", loaded.encoding());
        assertEquals(properties.getETag(), loaded.getEtag());
        assertEquals(metadata, loaded.getUserMetadata());
        assertEquals(URI.create(nativeBlobClient.getBlobUrl()), loaded.getPublicURI());
        assertEquals(toUtc(properties.getLastModified()), loaded.lastModified());
        assertNull(loaded.expires());

        Blob metadataOnly = client.getBlobMetadata(bucketName, blobKey);
        assertNull(metadataOnly.getContent());
        assertEquals(payload.length, metadataOnly.getSize());
        assertEquals("gzip", metadataOnly.encoding());
        assertEquals(properties.getETag(), metadataOnly.getEtag());
        assertEquals(metadata, metadataOnly.getUserMetadata());
        assertEquals(URI.create(nativeBlobClient.getBlobUrl()), metadataOnly.getPublicURI());
    }

    @Test
    void createBlobWithNullContentCreatesEmptyBlob() {
        String bucketName = createContainer("empty");
        String blobKey = "empty.bin";

        client.createBlob(bucketName, Blob.builder().bucket(bucketName).key(blobKey).content(null).build());

        BlobClient nativeBlobClient = nativeClient.getBlobContainerClient(bucketName).getBlobClient(blobKey);
        assertEquals(0L, nativeBlobClient.getProperties().getBlobSize());
        assertArrayEquals(new byte[0], nativeBlobClient.downloadContent().toBytes());
    }

    @Test
    void openBlobStreamAndByteRangeReturnExpectedContentAndValidateRanges() throws IOException {
        String bucketName = createContainer("stream");
        String blobKey = "payload.bin";
        byte[] payload = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        uploadNative(bucketName, blobKey, payload, null, null);

        try (InputStream stream = client.openBlobStream(bucketName, blobKey)) {
            assertArrayEquals(payload, stream.readAllBytes());
        }

        assertArrayEquals("34567".getBytes(StandardCharsets.UTF_8), client.getByteRange(bucketName, blobKey, 3, 7));

        assertThrows(
                IllegalArgumentException.class,
                () -> client.getByteRange(bucketName, blobKey, -1, 2)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> client.getByteRange(bucketName, blobKey, 8, 7)
        );
    }

    @Test
    void fileUploadsSupportPlainAndConfiguredWrites(@TempDir Path tempDir) throws IOException {
        String bucketName = createContainer("file");

        Path plainFile = tempDir.resolve("plain.txt");
        byte[] plainPayload = "plain file upload".getBytes(StandardCharsets.UTF_8);
        Files.write(plainFile, plainPayload);
        String plainEtag = client.createBlob(bucketName, "plain.txt", plainFile);

        BlobClient plainBlob = nativeClient.getBlobContainerClient(bucketName).getBlobClient("plain.txt");
        assertEquals(plainBlob.getProperties().getETag(), plainEtag);
        assertArrayEquals(plainPayload, plainBlob.downloadContent().toBytes());

        Path configuredFile = tempDir.resolve("configured.txt");
        byte[] configuredPayload = "configured file upload".getBytes(StandardCharsets.UTF_8);
        Files.write(configuredFile, configuredPayload);
        BlobWriteOptions options = BlobWriteOptions.builder()
                .encoding("br")
                .userMetadata(Map.of("source", "file", "mode", "configured"))
                .build();

        String configuredEtag = client.createBlob(bucketName, "configured.txt", configuredFile, options);

        BlobClient configuredBlob = nativeClient.getBlobContainerClient(bucketName).getBlobClient("configured.txt");
        BlobProperties configuredProperties = configuredBlob.getProperties();
        assertEquals(configuredProperties.getETag(), configuredEtag);
        assertArrayEquals(configuredPayload, configuredBlob.downloadContent().toBytes());
        assertEquals("br", configuredProperties.getContentEncoding());
        assertEquals(options.userMetadata(), configuredProperties.getMetadata());

        assertThrows(
                IllegalArgumentException.class,
                () -> client.createBlob(bucketName, "missing.txt", tempDir.resolve("missing.txt"))
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> client.createBlob(bucketName, "null.txt", null)
        );
    }

    @Test
    void streamUploadsSupportMetadataAndValidateArguments() {
        String bucketName = createContainer("streamupload");
        byte[] payload = "streamed upload body".getBytes(StandardCharsets.UTF_8);
        BlobWriteOptions options = BlobWriteOptions.builder()
                .encoding("deflate")
                .userMetadata(Map.of("source", "stream"))
                .build();

        String etag = client.createBlob(
                bucketName,
                "stream.txt",
                new ByteArrayInputStream(payload),
                payload.length,
                options
        );

        BlobClient nativeBlob = nativeClient.getBlobContainerClient(bucketName).getBlobClient("stream.txt");
        BlobProperties properties = nativeBlob.getProperties();
        assertEquals(properties.getETag(), etag);
        assertArrayEquals(payload, nativeBlob.downloadContent().toBytes());
        assertEquals("deflate", properties.getContentEncoding());
        assertEquals(options.userMetadata(), properties.getMetadata());

        assertThrows(
                IllegalArgumentException.class,
                () -> client.createBlob(bucketName, "null-stream.txt", null, 0, options)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> client.createBlob(bucketName, "negative.txt", new ByteArrayInputStream(payload), -1, options)
        );
    }

    @Test
    void streamUploadsRejectMismatchedContentLength() {
        String bucketName = createContainer("streammismatch");
        byte[] payload = "stream mismatch".getBytes(StandardCharsets.UTF_8);

        UbsaException shortStream = assertThrows(
                UbsaException.class,
                () -> client.createBlob(
                        bucketName,
                        "short.txt",
                        new ByteArrayInputStream(payload),
                        payload.length + 1L,
                        null
                )
        );
        assertFalse(nativeClient.getBlobContainerClient(bucketName).getBlobClient("short.txt").exists());
        assertNotNull(shortStream.getCause());

        UbsaException longStream = assertThrows(
                UbsaException.class,
                () -> client.createBlob(
                        bucketName,
                        "long.txt",
                        new ByteArrayInputStream(payload),
                        payload.length - 1L,
                        null
                )
        );
        assertFalse(nativeClient.getBlobContainerClient(bucketName).getBlobClient("long.txt").exists());
        assertNotNull(longStream.getCause());
    }

    @Test
    void deleteBlobIfExistsAndCopyBlobMatchNativeState() {
        String sourceBucket = createContainer("copy-src");
        String destinationBucket = createContainer("copy-dst");
        String sourceBlobKey = "source.txt";
        String destinationBlobKey = "copied.txt";
        byte[] payload = "copy me".getBytes(StandardCharsets.UTF_8);
        uploadNative(sourceBucket, sourceBlobKey, payload, Map.of("source", "native"), "gzip");

        String copiedEtag = client.copyBlob(sourceBucket, sourceBlobKey, destinationBucket, destinationBlobKey);

        BlobClient destinationBlob = nativeClient.getBlobContainerClient(destinationBucket).getBlobClient(destinationBlobKey);
        assertEquals(destinationBlob.getProperties().getETag(), copiedEtag);
        assertArrayEquals(payload, destinationBlob.downloadContent().toBytes());

        client.deleteBlobIfExists(destinationBucket, destinationBlobKey);
        assertFalse(destinationBlob.exists());
        assertDoesNotThrow(() -> client.deleteBlobIfExists(destinationBucket, destinationBlobKey));
    }

    @Test
    void listBlobsAndIterateBlobsMatchNativeListingWithPagination() {
        String bucketName = createContainer("listblobs");
        Map<String, String> metadata = Map.of("listed", "true");
        uploadNative(bucketName, "prefix/a.txt", "A".getBytes(StandardCharsets.UTF_8), metadata, null);
        uploadNative(bucketName, "prefix/b.txt", "BB".getBytes(StandardCharsets.UTF_8), metadata, null);
        uploadNative(bucketName, "prefix/c.txt", "CCC".getBytes(StandardCharsets.UTF_8), metadata, null);
        uploadNative(bucketName, "other/z.txt", "ignored".getBytes(StandardCharsets.UTF_8), metadata, null);

        ListBlobsOptions nativeOptions = new ListBlobsOptions()
                .setPrefix("prefix/")
                .setDetails(new BlobListDetails().setRetrieveMetadata(true));
        Iterator<PagedResponse<BlobItem>> nativePages = nativeClient.getBlobContainerClient(bucketName)
                .listBlobs(nativeOptions, null)
                .iterableByPage(2)
                .iterator();
        PagedResponse<BlobItem> nativeFirstPage = nativePages.next();
        PagedResponse<BlobItem> nativeSecondPage = nativePages.next();
        Map<String, BlobItem> nativeItemsByName = blobItemsByName(bucketName, nativeOptions);

        ListingPage<Blob> firstPage = client.listBlobs(bucketName, "prefix/", PageRequest.builder().pageSize(2).build());
        assertEquals(blobNames(nativeFirstPage.getElements()), blobKeys(firstPage.getItems()));
        assertEquals(nativeFirstPage.getContinuationToken(), firstPage.getNextContinuationToken());
        assertTrue(firstPage.hasNextPage());
        firstPage.getItems().forEach(item -> assertMatchesNativeBlob(bucketName, item, nativeItemsByName.get(item.getKey())));

        ListingPage<Blob> secondPage = client.listBlobs(
                bucketName,
                "prefix/",
                PageRequest.builder()
                        .pageSize(2)
                        .continuationToken(firstPage.getNextContinuationToken())
                        .build()
        );
        assertEquals(blobNames(nativeSecondPage.getElements()), blobKeys(secondPage.getItems()));
        assertEquals(nativeSecondPage.getContinuationToken(), secondPage.getNextContinuationToken());
        assertFalse(secondPage.hasNextPage());
        secondPage.getItems().forEach(item -> assertMatchesNativeBlob(bucketName, item, nativeItemsByName.get(item.getKey())));

        ListingPage<Blob> emptyPage = client.listBlobs(bucketName, "missing/", null);
        assertTrue(emptyPage.getItems().isEmpty());
        assertFalse(emptyPage.hasNextPage());
        assertNull(emptyPage.getNextContinuationToken());

        Set<String> iteratedKeys = new HashSet<>();
        client.iterateBlobs(bucketName, "prefix/", 1).forEach(blob -> iteratedKeys.add(blob.getKey()));
        assertEquals(Set.of("prefix/a.txt", "prefix/b.txt", "prefix/c.txt"), iteratedKeys);
    }

    @Test
    void listBucketsListAllBucketsAndIterateBucketsExposeCreatedContainers() {
        String prefix = bucketName("listbucket");
        String first = createContainer(prefix);
        String second = createContainer(prefix);
        String third = createContainer(prefix);

        Map<String, BlobContainerItem> nativeContainersByName = containerItemsByName(prefix);
        Set<String> expectedNames = nativeContainersByName.keySet();
        assertEquals(Set.of(first, second, third), expectedNames);

        PageRequest request = PageRequest.builder().pageSize(1).build();
        Set<String> pagedNames = new HashSet<>();
        while (true) {
            ListingPage<Bucket> page = client.listBuckets(request);
            page.getItems().stream()
                    .filter(bucket -> bucket.getName().startsWith(prefix))
                    .forEach(bucket -> {
                        pagedNames.add(bucket.getName());
                        assertMatchesNativeBucket(bucket, nativeContainersByName.get(bucket.getName()));
                    });
            if (!page.hasNextPage()) {
                break;
            }
            request = PageRequest.builder()
                    .pageSize(1)
                    .continuationToken(page.getNextContinuationToken())
                    .build();
        }
        assertEquals(expectedNames, pagedNames);

        Set<String> allBucketNames = client.listAllBuckets().stream()
                .map(Bucket::getName)
                .filter(name -> name.startsWith(prefix))
                .collect(Collectors.toSet());
        assertEquals(expectedNames, allBucketNames);

        Set<String> iteratedBucketNames = new HashSet<>();
        client.iterateBuckets(1).forEach(bucket -> {
            if (bucket.getName().startsWith(prefix)) {
                iteratedBucketNames.add(bucket.getName());
            }
        });
        assertEquals(expectedNames, iteratedBucketNames);
    }

    @Test
    void signedUrlsAllowDownloadingAndUploading() throws Exception {
        String bucketName = createContainer("sas");
        String getBlobKey = "download.txt";
        byte[] downloadPayload = "download through sas".getBytes(StandardCharsets.UTF_8);
        uploadNative(bucketName, getBlobKey, downloadPayload, null, null);

        URL getUrl = client.generateGetUrl(bucketName, getBlobKey, Duration.ofMinutes(5));
        assertArrayEquals(downloadPayload, readAllBytes(getUrl));

        byte[] uploadPayload = "uploaded through sas".getBytes(StandardCharsets.UTF_8);
        URL putUrl = client.generatePutUrl(bucketName, "upload.txt", Duration.ofMinutes(5), "text/plain");
        HttpRequest putRequest = HttpRequest.newBuilder(putUrl.toURI())
                .header("x-ms-blob-type", "BlockBlob")
                .header("Content-Type", "text/plain")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(uploadPayload))
                .build();
        HttpResponse<byte[]> putResponse = HTTP_CLIENT.send(putRequest, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

        BlobClient uploadedBlob = nativeClient.getBlobContainerClient(bucketName).getBlobClient("upload.txt");
        assertArrayEquals(uploadPayload, uploadedBlob.downloadContent().toBytes());

        assertThrows(
                IllegalArgumentException.class,
                () -> client.generateGetUrl(bucketName, getBlobKey, Duration.ZERO)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> client.generatePutUrl(bucketName, "bad.txt", Duration.ofSeconds(-1), "text/plain")
        );
    }

    @Test
    void signedUrlsAgainstMissingBucketReturnNotFound() throws Exception {
        String missingBucket = bucketName("missing-sas");

        URL getUrl = client.generateGetUrl(missingBucket, "missing.txt", Duration.ofMinutes(5));
        HttpRequest getRequest = HttpRequest.newBuilder(getUrl.toURI())
                .GET()
                .build();
        HttpResponse<byte[]> getResponse = HTTP_CLIENT.send(getRequest, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getResponse.statusCode());

        URL putUrl = client.generatePutUrl(missingBucket, "missing.txt", Duration.ofMinutes(5), "text/plain");
        HttpRequest putRequest = HttpRequest.newBuilder(putUrl.toURI())
                .header("x-ms-blob-type", "BlockBlob")
                .header("Content-Type", "text/plain")
                .PUT(HttpRequest.BodyPublishers.ofByteArray("missing bucket".getBytes(StandardCharsets.UTF_8)))
                .build();
        HttpResponse<byte[]> putResponse = HTTP_CLIENT.send(putRequest, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, putResponse.statusCode());
    }

    @Test
    void missingBlobOperationsRaiseProviderNeutralExceptions() {
        String bucketName = createContainer("missingblob");
        String missingKey = "missing.txt";

        BlobNotFoundException getBlob = assertThrows(BlobNotFoundException.class, () -> client.getBlob(bucketName, missingKey));
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, getBlob.getStatusCode());

        BlobNotFoundException metadata = assertThrows(BlobNotFoundException.class, () -> client.getBlobMetadata(bucketName, missingKey));
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, metadata.getStatusCode());

        BlobNotFoundException range = assertThrows(
                BlobNotFoundException.class,
                () -> client.getByteRange(bucketName, missingKey, 0, 2)
        );
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, range.getStatusCode());

        BlobNotFoundException stream = assertThrows(BlobNotFoundException.class, () -> {
            try (InputStream ignored = client.openBlobStream(bucketName, missingKey)) {
                ignored.readAllBytes();
            }
        });
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, stream.getStatusCode());
    }

    private String createContainer(String prefix) {
        String bucketName = bucketName(prefix);
        nativeClient.createBlobContainer(bucketName);
        createdContainers.add(bucketName);
        return bucketName;
    }

    private String bucketName(String prefix) {
        String normalized = prefix.toLowerCase().replaceAll("[^a-z0-9]", "");
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 18);
        return (normalized + suffix).substring(0, Math.min(normalized.length() + suffix.length(), 63));
    }

    private void uploadNative(
            String bucketName,
            String blobKey,
            byte[] content,
            Map<String, String> metadata,
            String encoding
    ) {
        BlobParallelUploadOptions options = new BlobParallelUploadOptions(BinaryData.fromBytes(content));
        if (metadata != null && !metadata.isEmpty()) {
            options.setMetadata(new LinkedHashMap<>(metadata));
        }
        if (encoding != null) {
            options.setHeaders(new BlobHttpHeaders().setContentEncoding(encoding));
        }
        nativeClient.getBlobContainerClient(bucketName)
                .getBlobClient(blobKey)
                .uploadWithResponse(options, null, null);
    }

    private static LocalDateTime toUtc(OffsetDateTime timestamp) {
        return timestamp == null ? null : timestamp.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private static List<String> blobNames(Iterable<BlobItem> items) {
        List<String> names = new ArrayList<>();
        items.forEach(item -> names.add(item.getName()));
        return names;
    }

    private static List<String> blobKeys(List<Blob> blobs) {
        return blobs.stream().map(Blob::getKey).toList();
    }

    private Map<String, BlobItem> blobItemsByName(String bucketName, ListBlobsOptions options) {
        Map<String, BlobItem> items = new LinkedHashMap<>();
        nativeClient.getBlobContainerClient(bucketName).listBlobs(options, null).forEach(item -> items.put(item.getName(), item));
        return items;
    }

    private Map<String, BlobContainerItem> containerItemsByName(String prefix) {
        Map<String, BlobContainerItem> items = new LinkedHashMap<>();
        PagedIterable<BlobContainerItem> containers = nativeClient.listBlobContainers();
        containers.forEach(container -> {
            if (container.getName().startsWith(prefix)) {
                items.put(container.getName(), container);
            }
        });
        return items;
    }

    private void assertMatchesNativeBlob(String bucketName, Blob blob, BlobItem nativeItem) {
        assertNotNull(nativeItem);
        BlobItemProperties properties = nativeItem.getProperties();
        BlobClient nativeBlobClient = nativeClient.getBlobContainerClient(bucketName).getBlobClient(nativeItem.getName());

        assertEquals(bucketName, blob.getBucket());
        assertEquals(nativeItem.getName(), blob.getKey());
        assertEquals(properties == null || properties.getContentLength() == null ? 0L : properties.getContentLength(), blob.getSize());
        assertEquals(properties == null ? null : properties.getContentEncoding(), blob.encoding());
        assertEquals(properties == null ? null : properties.getETag(), blob.getEtag());
        assertEquals(nativeItem.getMetadata(), blob.getUserMetadata());
        assertEquals(toUtc(properties == null ? null : properties.getLastModified()), blob.lastModified());
        assertEquals(toUtc(properties == null ? null : properties.getExpiryTime()), blob.expires());
        assertEquals(URI.create(nativeBlobClient.getBlobUrl()), blob.getPublicURI());
    }

    private void assertMatchesNativeBucket(Bucket bucket, BlobContainerItem nativeItem) {
        assertNotNull(nativeItem);
        BlobContainerItemProperties properties = nativeItem.getProperties();

        assertEquals(nativeItem.getName(), bucket.getName());
        assertEquals(URI.create(nativeClient.getBlobContainerClient(bucket.getName()).getBlobContainerUrl()), bucket.getPublicURI());
        assertEquals(toUtc(properties == null ? null : properties.getLastModified()), bucket.getLastModified());
        assertNull(bucket.getCreationDate());
    }

    private static byte[] readAllBytes(URL url) throws IOException {
        try (InputStream stream = url.openStream()) {
            return stream.readAllBytes();
        }
    }
}
