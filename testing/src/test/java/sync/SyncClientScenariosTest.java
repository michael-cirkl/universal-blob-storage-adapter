package sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SyncClientScenariosTest {
    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void storesAndBrowsesSampleProjectDocuments(SyncProviderFixture fixture, @TempDir Path tempDir) throws IOException {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("scenario-docs");
            String prefix = "project-alpha/";

            byte[] readmeContent = "# Alpha\nScenario sample".getBytes(StandardCharsets.UTF_8);
            context.client().createBlob(bucketName, Blob.builder()
                    .bucket(bucketName)
                    .key(prefix + "README.md")
                    .content(readmeContent)
                    .encoding("gzip")
                    .userMetadata(Map.of("type", "readme"))
                    .build());

            Path reportFile = tempDir.resolve("report.csv");
            byte[] reportContent = "id,value\n1,42\n2,84\n".getBytes(StandardCharsets.UTF_8);
            Files.write(reportFile, reportContent);
            context.client().createBlob(
                    bucketName,
                    prefix + "reports/report.csv",
                    reportFile,
                    BlobWriteOptions.builder()
                            .encoding("br")
                            .userMetadata(Map.of("type", "report"))
                            .build()
            );

            byte[] notesContent = "notes for the sprint review".getBytes(StandardCharsets.UTF_8);
            context.client().createBlob(
                    bucketName,
                    prefix + "notes/summary.txt",
                    new ByteArrayInputStream(notesContent),
                    notesContent.length,
                    BlobWriteOptions.builder().userMetadata(Map.of("type", "notes")).build()
            );

            ListingPage<Blob> firstPage = context.client().listBlobs(bucketName, prefix, PageRequest.builder().pageSize(2).build());
            assertEquals(2, firstPage.getItems().size());
            assertTrue(firstPage.hasNextPage());

            ListingPage<Blob> secondPage = context.client().listBlobs(
                    bucketName,
                    prefix,
                    PageRequest.builder().pageSize(2).continuationToken(firstPage.getNextContinuationToken()).build()
            );
            assertEquals(1, secondPage.getItems().size());
            assertFalse(secondPage.hasNextPage());

            List<String> listedKeys = new ArrayList<>();
            firstPage.getItems().forEach(blob -> listedKeys.add(blob.getKey()));
            secondPage.getItems().forEach(blob -> listedKeys.add(blob.getKey()));
            assertEquals(
                    List.of(prefix + "README.md", prefix + "notes/summary.txt", prefix + "reports/report.csv"),
                    listedKeys.stream().sorted().toList()
            );

            Blob metadataOnly = context.client().getBlobMetadata(bucketName, prefix + "reports/report.csv");
            assertEquals(bucketName, metadataOnly.getBucket());
            assertEquals(prefix + "reports/report.csv", metadataOnly.getKey());
            assertEquals(reportContent.length, metadataOnly.getSize());
            assertEquals("br", metadataOnly.encoding());
            assertEquals(Map.of("type", "report"), metadataOnly.getUserMetadata());
            assertNull(metadataOnly.getContent());
            assertNotNull(metadataOnly.lastModified());
            assertNotNull(metadataOnly.getPublicURI());

            Blob readme = context.client().getBlob(bucketName, prefix + "README.md");
            assertArrayEquals(readmeContent, readme.getContent());

            try (InputStream notesStream = context.client().openBlobStream(bucketName, prefix + "notes/summary.txt")) {
                assertArrayEquals(notesContent, notesStream.readAllBytes());
            }

            assertArrayEquals("Alpha".getBytes(StandardCharsets.UTF_8), context.client().getByteRange(bucketName, prefix + "README.md", 2, 6));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void managesBucketLifecycleFromCreationToMissingState(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.newBucketName("scenario-bucket");

            assertFalse(context.client().bucketExists(bucketName));

            context.client().createBucket(Bucket.builder().name(bucketName).build());
            assertTrue(context.client().bucketExists(bucketName));

            context.client().deleteBucket(bucketName);
            assertFalse(context.client().bucketExists(bucketName));

            context.client().deleteBucketIfExists(bucketName);
            assertFalse(context.client().bucketExists(bucketName));

            UbsaException missing = assertThrows(
                    UbsaException.class,
                    () -> context.client().deleteBucket(bucketName)
            );
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, missing.getStatusCode());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void acceptsExternalStyleUploadAndDownloadThroughSignedUrls(SyncProviderFixture fixture) throws Exception {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("scenario-signed");
            String blobKey = "incoming/media/photo.txt";
            byte[] payload = "uploaded through signed put".getBytes(StandardCharsets.UTF_8);

            URL putUrl = context.client().generatePutUrl(bucketName, blobKey, Duration.ofMinutes(5), "text/plain");
            context.writeRequired(putUrl, "text/plain", payload);

            assertTrue(context.client().blobExists(bucketName, blobKey));

            Blob metadata = context.client().getBlobMetadata(bucketName, blobKey);
            assertEquals(bucketName, metadata.getBucket());
            assertEquals(blobKey, metadata.getKey());
            assertEquals(payload.length, metadata.getSize());
            assertNotNull(metadata.getPublicURI());

            URL getUrl = context.client().generateGetUrl(bucketName, blobKey, Duration.ofMinutes(5));
            assertArrayEquals(payload, context.readRequired(getUrl));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void copiesBlobThenDeletesBlobChecksExistenceAndDeletesArchiveBucket(SyncProviderFixture fixture) {
        try (SyncTestContext context = fixture.openContext()) {
            String sourceBucket = context.createBucket("scenario-copy-src");
            String archiveBucket = context.createBucket("scenario-copy-archive");
            String sourceBlobKey = "orders/2026-04/report.json";
            String copiedBlobKey = "archive/orders/2026-04/report.json";
            byte[] payload = "{\"order\":42,\"status\":\"done\"}".getBytes(StandardCharsets.UTF_8);
            Map<String, String> metadata = Map.of("source", "orders", "stage", "archive");

            context.client().createBlob(sourceBucket, Blob.builder()
                    .bucket(sourceBucket)
                    .key(sourceBlobKey)
                    .content(payload)
                    .encoding("gzip")
                    .userMetadata(metadata)
                    .build());

            context.client().copyBlob(sourceBucket, sourceBlobKey, archiveBucket, copiedBlobKey);

            Blob copiedBlob = context.client().getBlob(archiveBucket, copiedBlobKey);
            assertArrayEquals(payload, copiedBlob.getContent());
            assertEquals("gzip", copiedBlob.encoding());
            assertEquals(metadata, copiedBlob.getUserMetadata());

            context.client().deleteBlobIfExists(archiveBucket, copiedBlobKey);
            assertFalse(context.client().blobExists(archiveBucket, copiedBlobKey));

            context.client().deleteBucket(archiveBucket);
            assertFalse(context.client().bucketExists(archiveBucket));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(SyncProviderFixtureArgumentsProvider.class)
    void overwritesStoredDocumentAndExposesLatestContentMetadataAndListingState(SyncProviderFixture fixture, @TempDir Path tempDir) throws IOException {
        try (SyncTestContext context = fixture.openContext()) {
            String bucketName = context.createBucket("scenario-overwrite");
            String blobKey = "workspace/config/settings.json";
            String prefix = "workspace/config/";

            byte[] initialContent = "{\"mode\":\"draft\",\"version\":1}".getBytes(StandardCharsets.UTF_8);
            context.client().createBlob(
                    bucketName,
                    Blob.builder()
                            .bucket(bucketName)
                            .key(blobKey)
                            .content(initialContent)
                            .encoding("gzip")
                            .userMetadata(Map.of("revision", "v1", "owner", "team-a"))
                            .build()
            );

            Path updatedFile = tempDir.resolve("settings.json");
            byte[] updatedContent = "{\"mode\":\"published\",\"version\":2}".getBytes(StandardCharsets.UTF_8);
            Files.write(updatedFile, updatedContent);
            context.client().createBlob(
                    bucketName,
                    blobKey,
                    updatedFile,
                    BlobWriteOptions.builder()
                            .encoding("br")
                            .userMetadata(Map.of("revision", "v2", "owner", "team-b"))
                            .build()
            );

            Blob metadata = context.client().getBlobMetadata(bucketName, blobKey);
            assertEquals(bucketName, metadata.getBucket());
            assertEquals(blobKey, metadata.getKey());
            assertEquals(updatedContent.length, metadata.getSize());
            assertEquals("br", metadata.encoding());
            assertEquals(Map.of("revision", "v2", "owner", "team-b"), metadata.getUserMetadata());
            assertNull(metadata.getContent());
            assertNotNull(metadata.lastModified());

            Blob blob = context.client().getBlob(bucketName, blobKey);
            assertArrayEquals(updatedContent, blob.getContent());
            assertEquals("br", blob.encoding());
            assertEquals(Map.of("revision", "v2", "owner", "team-b"), blob.getUserMetadata());

            ListingPage<Blob> page = context.client().listBlobs(bucketName, prefix, PageRequest.builder().pageSize(10).build());
            assertEquals(1, page.getItems().size());
            Blob listedBlob = page.getItems().get(0);
            assertEquals(blobKey, listedBlob.getKey());
            assertEquals(updatedContent.length, listedBlob.getSize());

            Blob listedMetadata = context.client().getBlobMetadata(bucketName, listedBlob.getKey());
            assertEquals("br", listedMetadata.encoding());
            assertEquals(Map.of("revision", "v2", "owner", "team-b"), listedMetadata.getUserMetadata());
        }
    }
}
