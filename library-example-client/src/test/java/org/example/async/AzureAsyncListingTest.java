package org.example.async;

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.PagedFlux;
import com.azure.core.util.IterableStream;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.async.AzureAsyncClientImpl;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureAsyncListingTest {
    @Test
    void listBucketsReturnsSinglePageWithoutCollectingAllPages() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        @SuppressWarnings("unchecked")
        PagedFlux<BlobContainerItem> containers = mock(PagedFlux.class);
        @SuppressWarnings("unchecked")
        PagedResponse<BlobContainerItem> page = mock(PagedResponse.class);
        BlobContainerItem containerItem = mock(BlobContainerItem.class);
        BlobContainerItemProperties properties = mock(BlobContainerItemProperties.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        OffsetDateTime lastModified = OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);

        when(serviceClient.listBlobContainers()).thenReturn(containers);
        when(containers.byPage(null, 5)).thenReturn(Flux.just(page));
        when(page.getElements()).thenReturn(new IterableStream<>(List.of(containerItem)));
        when(page.getContinuationToken()).thenReturn("page-2");
        when(containerItem.getName()).thenReturn("bucket");
        when(containerItem.getProperties()).thenReturn(properties);
        when(properties.getLastModified()).thenReturn(lastModified);
        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobContainerUrl()).thenReturn("https://example.test/bucket");

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        ListingPage<Bucket> buckets = adapter.listBuckets(PageRequest.builder().pageSize(5).build()).get();

        assertEquals(1, buckets.getItems().size());
        Bucket bucket = buckets.getItems().get(0);
        assertEquals("bucket", bucket.getName());
        assertNull(bucket.getCreationDate());
        assertEquals(lastModified.toLocalDateTime(), bucket.getLastModified());
        assertTrue(buckets.hasNextPage());
        assertEquals("page-2", buckets.getNextContinuationToken());
        verify(containers, never()).collectList();
    }

    @Test
    void listBlobsRequestsMetadataAndMapsIt() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        BlobAsyncClient blobClient = mock(BlobAsyncClient.class);
        @SuppressWarnings("unchecked")
        PagedFlux<BlobItem> blobs = mock(PagedFlux.class);
        @SuppressWarnings("unchecked")
        PagedResponse<BlobItem> page = mock(PagedResponse.class);
        BlobItem blobItem = mock(BlobItem.class);
        BlobItemProperties properties = mock(BlobItemProperties.class);
        OffsetDateTime lastModified = OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);

        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.listBlobs(any(ListBlobsOptions.class), isNull())).thenAnswer(invocation -> {
            ListBlobsOptions options = invocation.getArgument(0);
            assertTrue(options.getDetails().getRetrieveMetadata());
            assertEquals("prefix/", options.getPrefix());
            return blobs;
        });
        when(blobs.byPage(null, 5)).thenReturn(Flux.just(page));
        when(page.getElements()).thenReturn(new IterableStream<>(List.of(blobItem)));
        when(blobItem.getName()).thenReturn("prefix/blob");
        when(blobItem.getProperties()).thenReturn(properties);
        when(blobItem.getMetadata()).thenReturn(Map.of("env", "test"));
        when(properties.getContentLength()).thenReturn(123L);
        when(properties.getLastModified()).thenReturn(lastModified);
        when(properties.getContentEncoding()).thenReturn("gzip");
        when(properties.getETag()).thenReturn("etag-1");
        when(containerClient.getBlobAsyncClient("prefix/blob")).thenReturn(blobClient);
        when(blobClient.getBlobUrl()).thenReturn("https://example.test/bucket/prefix/blob");

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        ListingPage<Blob> listedBlobs = adapter.listBlobs(
                "bucket",
                "prefix/",
                PageRequest.builder().pageSize(5).build()
        ).get();

        assertEquals(1, listedBlobs.getItems().size());
        Blob listedBlob = listedBlobs.getItems().get(0);
        assertEquals("prefix/blob", listedBlob.getKey());
        assertEquals(123L, listedBlob.getSize());
        assertEquals(lastModified.toLocalDateTime(), listedBlob.lastModified());
        assertEquals("gzip", listedBlob.encoding());
        assertEquals("etag-1", listedBlob.getEtag());
        assertEquals(Map.of("env", "test"), listedBlob.getUserMetadata());
    }
}
