package org.example.sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.client.sync.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsSyncPaginationTest {
    @Test
    void listBlobsByPrefixReadsAllPagesInOrder() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        ListObjectsV2Response firstPage = ListObjectsV2Response.builder()
                .isTruncated(true)
                .nextContinuationToken("page-2")
                .contents(S3Object.builder().key("prefix/blob-1").size(1L).build())
                .build();
        ListObjectsV2Response secondPage = ListObjectsV2Response.builder()
                .isTruncated(false)
                .contents(S3Object.builder().key("prefix/blob-2").size(2L).build())
                .build();

        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenAnswer(invocation -> {
                    ListObjectsV2Request request = invocation.getArgument(0);
                    return request.continuationToken() == null ? firstPage : secondPage;
                });

        List<Blob> blobs = adapter.listBlobsByPrefix("bucket", "prefix/");

        assertEquals(2, blobs.size());
        assertEquals(List.of("prefix/blob-1", "prefix/blob-2"), blobs.stream().map(Blob::getKey).toList());
    }

    @Test
    void listBlobsByPrefixPreservesDuplicateEntries() {
        S3Client client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(client);

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .isTruncated(false)
                .contents(
                        S3Object.builder().key("prefix/blob-1").size(1L).build(),
                        S3Object.builder().key("prefix/blob-1").size(1L).build()
                )
                .build();

        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        List<Blob> blobs = adapter.listBlobsByPrefix("bucket", "prefix/");

        assertEquals(2, blobs.size());
        assertEquals(List.of("prefix/blob-1", "prefix/blob-1"), blobs.stream().map(Blob::getKey).toList());
    }
}
