package michaelcirkl.ubsa.client.streaming;

import com.google.cloud.storage.BlobInfo;
import michaelcirkl.ubsa.Blob;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WriteOptionsMappersTest {
    @Test
    void mapsWriteOptionsToAwsAndGcpBuilders() {
        BlobWriteOptions options = BlobWriteOptions.builder()
                .encoding("gzip")
                .userMetadata(Map.of("a", "b"))
                .build();

        PutObjectRequest.Builder awsBuilder = PutObjectRequest.builder().bucket("b").key("k");
        WriteOptionsMappers.applyOptionsToAwsPutObject(awsBuilder, options);
        PutObjectRequest awsRequest = awsBuilder.build();
        assertEquals("gzip", awsRequest.contentEncoding());
        assertEquals("b", awsRequest.metadata().get("a"));

        BlobInfo.Builder gcpBuilder = BlobInfo.newBuilder("bucket", "blob");
        WriteOptionsMappers.applyOptionsToGcpBlobInfo(gcpBuilder, options);
        BlobInfo blobInfo = gcpBuilder.build();
        assertEquals("gzip", blobInfo.getContentEncoding());
        assertEquals("b", blobInfo.getMetadata().get("a"));
    }

    @Test
    void mapsBlobToAwsAndAzureValues() {
        Blob blob = Blob.builder()
                .bucket("bucket")
                .key("blob")
                .encoding("gzip")
                .userMetadata(Map.of("k", "v"))
                .content("data".getBytes())
                .build();

        PutObjectRequest.Builder awsBuilder = PutObjectRequest.builder().bucket("bucket").key("blob");
        WriteOptionsMappers.applyBlobToAwsPutObject(awsBuilder, blob);
        PutObjectRequest request = awsBuilder.build();
        assertEquals("gzip", request.contentEncoding());
        assertEquals("v", request.metadata().get("k"));

        var uploadOptions = WriteOptionsMappers.buildAzureUploadOptions(blob);
        assertEquals("gzip", uploadOptions.getHeaders().getContentEncoding());
        assertEquals("v", uploadOptions.getMetadata().get("k"));
    }

    @Test
    void mapsAzureHeadersAndMetadata() {
        BlobWriteOptions empty = BlobWriteOptions.builder().encoding(" ").build();
        assertNull(WriteOptionsMappers.toAzureHeaders(empty));
        assertNull(WriteOptionsMappers.toAzureMetadata(empty));
    }
}
