package org.example.async;

import com.azure.core.http.rest.PagedFlux;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobContainerItemProperties;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.client.async.AzureAsyncClientImpl;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AzureAsyncListingTest {
    @Test
    void listAllBucketsLeavesCreationDateUnknown() throws Exception {
        BlobServiceAsyncClient serviceClient = mock(BlobServiceAsyncClient.class);
        @SuppressWarnings("unchecked")
        PagedFlux<BlobContainerItem> containers = mock(PagedFlux.class);
        BlobContainerItem containerItem = mock(BlobContainerItem.class);
        BlobContainerItemProperties properties = mock(BlobContainerItemProperties.class);
        BlobContainerAsyncClient containerClient = mock(BlobContainerAsyncClient.class);
        OffsetDateTime lastModified = OffsetDateTime.of(2025, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);

        when(serviceClient.listBlobContainers()).thenReturn(containers);
        when(containers.collectList()).thenReturn(Mono.just(List.of(containerItem)));
        when(containerItem.getName()).thenReturn("bucket");
        when(containerItem.getProperties()).thenReturn(properties);
        when(properties.getLastModified()).thenReturn(lastModified);
        when(serviceClient.getBlobContainerAsyncClient("bucket")).thenReturn(containerClient);
        when(containerClient.getBlobContainerUrl()).thenReturn("https://example.test/bucket");

        AzureAsyncClientImpl adapter = new AzureAsyncClientImpl(serviceClient);
        Set<Bucket> buckets = adapter.listAllBuckets().get();

        assertEquals(1, buckets.size());
        Bucket bucket = buckets.iterator().next();
        assertEquals("bucket", bucket.getName());
        assertNull(bucket.getCreationDate());
        assertEquals(lastModified.toLocalDateTime(), bucket.getLastModified());
    }
}
