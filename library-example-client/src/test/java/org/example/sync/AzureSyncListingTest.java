package org.example.sync;

import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.IterableStream;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.sync.AzureSyncClientImpl;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureSyncListingTest {
    @Test
    void listBucketsReturnsSinglePageWithoutEagerIteration() {
        BlobServiceClient serviceClient = mock(BlobServiceClient.class);
        @SuppressWarnings("unchecked")
        PagedIterable<BlobContainerItem> containers = mock(PagedIterable.class);
        @SuppressWarnings("unchecked")
        PagedResponse<BlobContainerItem> page = mock(PagedResponse.class);
        BlobContainerItem containerItem = mock(BlobContainerItem.class);
        BlobContainerItemProperties properties = mock(BlobContainerItemProperties.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);
        OffsetDateTime lastModified = OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);

        when(serviceClient.listBlobContainers()).thenReturn(containers);
        when(containers.iterableByPage(null, 5)).thenReturn(List.of(page));
        when(page.getElements()).thenReturn(new IterableStream<>(List.of(containerItem)));
        when(page.getContinuationToken()).thenReturn("page-2");
        when(containerItem.getName()).thenReturn("bucket");
        when(containerItem.getProperties()).thenReturn(properties);
        when(properties.getLastModified()).thenReturn(lastModified);
        when(serviceClient.getBlobContainerClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobContainerUrl()).thenReturn("https://example.test/bucket");

        AzureSyncClientImpl adapter = new AzureSyncClientImpl(serviceClient);
        ListingPage<Bucket> buckets = adapter.listBuckets(PageRequest.builder().pageSize(5).build());

        assertEquals(1, buckets.getItems().size());
        Bucket bucket = buckets.getItems().get(0);
        assertEquals("bucket", bucket.getName());
        assertNull(bucket.getCreationDate());
        assertEquals(lastModified.toLocalDateTime(), bucket.getLastModified());
        assertTrue(buckets.hasNextPage());
        assertEquals("page-2", buckets.getNextContinuationToken());
        verify(containers, never()).forEach(org.mockito.ArgumentMatchers.any());
    }
}
