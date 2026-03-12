package org.example.streaming;

import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.sync.AWSSyncClientImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class AwsSyncStreamingTest {
    @Test
    void openBlobStreamStreamsDataFromSdkStream() throws Exception {
        S3Client s3Client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(s3Client);
        byte[] expected = "aws-sync-stream".getBytes();

        ResponseInputStream<GetObjectResponse> stream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                new ByteArrayInputStream(expected)
        );
        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(stream);

        InputStream opened = adapter.openBlobStream("bucket", "blob");
        assertArrayEquals(expected, StreamingTestSupport.readAll(opened));
    }

    @Test
    void createBlobStreamingUsesDeclaredLengthAndReturnsEtag() {
        S3Client s3Client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(s3Client);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("etag-aws-sync").build());

        byte[] content = "stream-upload".getBytes();
        String etag = adapter.createBlob(
                "bucket",
                "blob",
                new ByteArrayInputStream(content),
                content.length,
                BlobWriteOptions.builder().encoding("gzip").build()
        );

        assertEquals("etag-aws-sync", etag);
    }

    @Test
    void createBlobFromFileUsesRequestBodyFromFile(@TempDir Path tempDir) throws Exception {
        S3Client s3Client = mock(S3Client.class);
        AWSSyncClientImpl adapter = new AWSSyncClientImpl(s3Client);
        Path sourceFile = Files.writeString(tempDir.resolve("aws-sync.txt"), "aws-sync-file");

        try (MockedStatic<RequestBody> requestBodyMock = mockStatic(RequestBody.class)) {
            RequestBody requestBody = mock(RequestBody.class);
            requestBodyMock.when(() -> RequestBody.fromFile(sourceFile)).thenReturn(requestBody);
            when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenReturn(PutObjectResponse.builder().eTag("etag-aws-sync-file").build());

            String etag = adapter.createBlob("bucket", "blob", sourceFile);
            assertEquals("etag-aws-sync-file", etag);

            requestBodyMock.verify(() -> RequestBody.fromFile(sourceFile));
            ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
            org.mockito.Mockito.verify(s3Client).putObject(requestCaptor.capture(), org.mockito.Mockito.same(requestBody));
            assertEquals("bucket", requestCaptor.getValue().bucket());
            assertEquals("blob", requestCaptor.getValue().key());
        }
    }
}
