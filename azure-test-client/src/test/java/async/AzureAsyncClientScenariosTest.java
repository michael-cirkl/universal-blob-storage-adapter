package async;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.exception.types.BucketNotFoundException;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import support.AsyncTestSupport;
import support.AzureSampleClientSupport;

import java.io.IOException;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AzureAsyncClientScenariosTest {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    private BlobServiceClient nativeClient;
    private BlobServiceAsyncClient nativeAsyncClient;
    private BlobStorageAsyncClient client;
    private final List<String> createdContainers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        nativeClient = AzureSampleClientSupport.createNativeSyncClient();
        nativeAsyncClient = AzureSampleClientSupport.createNativeAsyncClient();
        client = BlobStorageClientFactory.getAsyncClient(nativeAsyncClient);
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
    void storesAndBrowsesSampleProjectDocuments(@TempDir Path tempDir) throws IOException {
        String bucketName = createContainer("scenario-docs");
        String prefix = "project-alpha/";

        byte[] readmeContent = "# Alpha\nScenario sample".getBytes(StandardCharsets.UTF_8);
        AsyncTestSupport.await(client.createBlob(bucketName, Blob.builder()
                .bucket(bucketName)
                .key(prefix + "README.md")
                .content(readmeContent)
                .encoding("gzip")
                .userMetadata(Map.of("type", "readme"))
                .build()));

        Path reportFile = tempDir.resolve("report.csv");
        byte[] reportContent = "id,value\n1,42\n2,84\n".getBytes(StandardCharsets.UTF_8);
        Files.write(reportFile, reportContent);
        AsyncTestSupport.await(client.createBlob(
                bucketName,
                prefix + "reports/report.csv",
                reportFile,
                BlobWriteOptions.builder()
                        .encoding("br")
                        .userMetadata(Map.of("type", "report"))
                        .build()
        ));

        byte[] notesContent = "notes for the sprint review".getBytes(StandardCharsets.UTF_8);
        AsyncTestSupport.await(client.createBlob(
                bucketName,
                prefix + "notes/summary.txt",
                AsyncTestSupport.publisherOf(notesContent),
                notesContent.length,
                BlobWriteOptions.builder()
                        .userMetadata(Map.of("type", "notes"))
                        .build()
        ));

        List<String> nativeListedKeys = new ArrayList<>();
        nativeClient.getBlobContainerClient(bucketName)
                .listBlobs(new ListBlobsOptions().setPrefix(prefix), null)
                .forEach(item -> nativeListedKeys.add(item.getName()));
        assertEquals(
                List.of(
                        prefix + "README.md",
                        prefix + "notes/summary.txt",
                        prefix + "reports/report.csv"
                ),
                nativeListedKeys.stream().sorted().toList()
        );

        ListingPage<Blob> firstPage = AsyncTestSupport.await(client.listBlobs(bucketName, prefix, PageRequest.builder().pageSize(2).build()));
        assertEquals(2, firstPage.getItems().size());
        assertTrue(firstPage.hasNextPage());

        ListingPage<Blob> secondPage = AsyncTestSupport.await(client.listBlobs(
                bucketName,
                prefix,
                PageRequest.builder()
                        .pageSize(2)
                        .continuationToken(firstPage.getNextContinuationToken())
                        .build()
        ));
        assertEquals(1, secondPage.getItems().size());
        assertFalse(secondPage.hasNextPage());

        List<String> listedKeys = new ArrayList<>();
        firstPage.getItems().forEach(blob -> listedKeys.add(blob.getKey()));
        secondPage.getItems().forEach(blob -> listedKeys.add(blob.getKey()));
        assertEquals(
                List.of(
                        prefix + "README.md",
                        prefix + "notes/summary.txt",
                        prefix + "reports/report.csv"
                ),
                listedKeys.stream().sorted().toList()
        );

        Blob metadataOnly = AsyncTestSupport.await(client.getBlobMetadata(bucketName, prefix + "reports/report.csv"));
        assertEquals(bucketName, metadataOnly.getBucket());
        assertEquals(prefix + "reports/report.csv", metadataOnly.getKey());
        assertEquals(reportContent.length, metadataOnly.getSize());
        assertEquals("br", metadataOnly.encoding());
        assertEquals(Map.of("type", "report"), metadataOnly.getUserMetadata());
        assertNull(metadataOnly.getContent());
        assertNotNull(metadataOnly.lastModified());
        assertEquals(URI.create(nativeClient.getBlobContainerClient(bucketName).getBlobClient(prefix + "reports/report.csv").getBlobUrl()), metadataOnly.getPublicURI());

        Blob readme = AsyncTestSupport.await(client.getBlob(bucketName, prefix + "README.md"));
        assertArrayEquals(readmeContent, readme.getContent());

        assertArrayEquals(notesContent, AsyncTestSupport.awaitBytes(client.openBlobStream(bucketName, prefix + "notes/summary.txt")));
        assertArrayEquals("Alpha".getBytes(StandardCharsets.UTF_8), AsyncTestSupport.await(client.getByteRange(bucketName, prefix + "README.md", 2, 6)));
    }

    @Test
    void managesBucketLifecycleFromCreationToMissingState() {
        String bucketName = bucketName("scenario-bucket");

        assertFalse(AsyncTestSupport.await(client.bucketExists(bucketName)));

        AsyncTestSupport.await(client.createBucket(Bucket.builder().name(bucketName).build()));
        createdContainers.add(bucketName);

        assertTrue(AsyncTestSupport.await(client.bucketExists(bucketName)));
        assertTrue(nativeClient.getBlobContainerClient(bucketName).exists());

        AsyncTestSupport.await(client.deleteBucket(bucketName));
        assertFalse(nativeClient.getBlobContainerClient(bucketName).exists());

        AsyncTestSupport.await(client.deleteBucketIfExists(bucketName));
        assertFalse(AsyncTestSupport.await(client.bucketExists(bucketName)));

        BucketNotFoundException missing = AsyncTestSupport.assertFutureThrows(
                BucketNotFoundException.class,
                client.deleteBucket(bucketName)
        );
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, missing.getStatusCode());
    }

    @Test
    void acceptsExternalStyleUploadAndDownloadThroughSignedUrls() throws Exception {
        String bucketName = createContainer("scenario-sas");
        String blobKey = "incoming/media/photo.txt";
        byte[] payload = "uploaded through signed put".getBytes(StandardCharsets.UTF_8);

        URL putUrl = client.generatePutUrl(bucketName, blobKey, Duration.ofMinutes(5), "text/plain");
        HttpRequest putRequest = HttpRequest.newBuilder(putUrl.toURI())
                .header("x-ms-blob-type", "BlockBlob")
                .header("Content-Type", "text/plain")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        HttpResponse<byte[]> putResponse = HTTP_CLIENT.send(putRequest, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(HttpURLConnection.HTTP_CREATED, putResponse.statusCode());

        assertTrue(AsyncTestSupport.await(client.blobExists(bucketName, blobKey)));

        Blob metadata = AsyncTestSupport.await(client.getBlobMetadata(bucketName, blobKey));
        assertEquals(bucketName, metadata.getBucket());
        assertEquals(blobKey, metadata.getKey());
        assertEquals(payload.length, metadata.getSize());
        assertEquals(URI.create(nativeClient.getBlobContainerClient(bucketName).getBlobClient(blobKey).getBlobUrl()), metadata.getPublicURI());

        URL getUrl = client.generateGetUrl(bucketName, blobKey, Duration.ofMinutes(5));
        HttpRequest getRequest = HttpRequest.newBuilder(getUrl.toURI())
                .GET()
                .build();
        HttpResponse<byte[]> getResponse = HTTP_CLIENT.send(getRequest, HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(HttpURLConnection.HTTP_OK, getResponse.statusCode());
        assertArrayEquals(payload, getResponse.body());
    }

    @Test
    void copiesBlobThenDeletesBlobChecksExistenceAndDeletesArchiveBucket() {
        String sourceBucket = createContainer("scenario-copy-src");
        String archiveBucket = createContainer("scenario-copy-archive");
        String sourceBlobKey = "orders/2026-04/report.json";
        String copiedBlobKey = "archive/orders/2026-04/report.json";
        byte[] payload = "{\"order\":42,\"status\":\"done\"}".getBytes(StandardCharsets.UTF_8);
        Map<String, String> metadata = Map.of("source", "orders", "stage", "archive");

        uploadNative(sourceBucket, sourceBlobKey, payload, metadata, "gzip");

        AsyncTestSupport.await(client.copyBlob(sourceBucket, sourceBlobKey, archiveBucket, copiedBlobKey));

        BlobClient copiedBlob = nativeClient.getBlobContainerClient(archiveBucket).getBlobClient(copiedBlobKey);
        BlobProperties copiedProperties = copiedBlob.getProperties();
        assertArrayEquals(payload, copiedBlob.downloadContent().toBytes());
        assertEquals("gzip", copiedProperties.getContentEncoding());
        assertEquals(metadata, copiedProperties.getMetadata());

        AsyncTestSupport.await(client.deleteBlobIfExists(archiveBucket, copiedBlobKey));
        assertFalse(copiedBlob.exists());
        assertFalse(AsyncTestSupport.await(client.blobExists(archiveBucket, copiedBlobKey)));

        AsyncTestSupport.await(client.deleteBucket(archiveBucket));
        assertFalse(nativeClient.getBlobContainerClient(archiveBucket).exists());
    }

    @Test
    void overwritesStoredDocumentAndExposesLatestContentMetadataAndListingState(@TempDir Path tempDir) throws IOException {
        String bucketName = createContainer("scenario-overwrite");
        String blobKey = "workspace/config/settings.json";
        String prefix = "workspace/config/";

        byte[] initialContent = "{\"mode\":\"draft\",\"version\":1}".getBytes(StandardCharsets.UTF_8);
        AsyncTestSupport.await(client.createBlob(
                bucketName,
                Blob.builder()
                        .bucket(bucketName)
                        .key(blobKey)
                        .content(initialContent)
                        .encoding("gzip")
                        .userMetadata(Map.of("revision", "v1", "owner", "team-a"))
                        .build()
        ));

        Path updatedFile = tempDir.resolve("settings.json");
        byte[] updatedContent = "{\"mode\":\"published\",\"version\":2}".getBytes(StandardCharsets.UTF_8);
        Files.write(updatedFile, updatedContent);
        AsyncTestSupport.await(client.createBlob(
                bucketName,
                blobKey,
                updatedFile,
                BlobWriteOptions.builder()
                        .encoding("br")
                        .userMetadata(Map.of("revision", "v2", "owner", "team-b"))
                        .build()
        ));

        List<String> nativeListedKeys = new ArrayList<>();
        nativeClient.getBlobContainerClient(bucketName)
                .listBlobs(new ListBlobsOptions().setPrefix(prefix), null)
                .forEach(item -> nativeListedKeys.add(item.getName()));
        assertEquals(List.of(blobKey), nativeListedKeys);

        Blob metadata = AsyncTestSupport.await(client.getBlobMetadata(bucketName, blobKey));
        assertEquals(bucketName, metadata.getBucket());
        assertEquals(blobKey, metadata.getKey());
        assertEquals(updatedContent.length, metadata.getSize());
        assertEquals("br", metadata.encoding());
        assertEquals(Map.of("revision", "v2", "owner", "team-b"), metadata.getUserMetadata());
        assertNull(metadata.getContent());
        assertNotNull(metadata.lastModified());

        Blob blob = AsyncTestSupport.await(client.getBlob(bucketName, blobKey));
        assertArrayEquals(updatedContent, blob.getContent());
        assertEquals("br", blob.encoding());
        assertEquals(Map.of("revision", "v2", "owner", "team-b"), blob.getUserMetadata());

        ListingPage<Blob> page = AsyncTestSupport.await(client.listBlobs(bucketName, prefix, PageRequest.builder().pageSize(10).build()));
        assertEquals(1, page.getItems().size());
        Blob listedBlob = page.getItems().getFirst();
        assertEquals(blobKey, listedBlob.getKey());
        assertEquals(updatedContent.length, listedBlob.getSize());
        assertEquals("br", listedBlob.encoding());
        assertEquals(Map.of("revision", "v2", "owner", "team-b"), listedBlob.getUserMetadata());

        BlobClient nativeBlob = nativeClient.getBlobContainerClient(bucketName).getBlobClient(blobKey);
        BlobProperties nativeProperties = nativeBlob.getProperties();
        assertArrayEquals(updatedContent, nativeBlob.downloadContent().toBytes());
        assertEquals("br", nativeProperties.getContentEncoding());
        assertEquals(Map.of("revision", "v2", "owner", "team-b"), nativeProperties.getMetadata());
    }

    private String createContainer(String prefix) {
        String bucketName = bucketName(prefix);
        AsyncTestSupport.await(client.createBucket(Bucket.builder().name(bucketName).build()));
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
}
