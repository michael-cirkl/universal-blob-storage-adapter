package io.github.michaelcirkl.ubsa.client.streaming;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.google.cloud.storage.BlobInfo;
import io.github.michaelcirkl.ubsa.Blob;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.time.ZoneOffset;
import java.util.Map;

public final class WriteOptionsMappers {
    private WriteOptionsMappers() {
    }

    public static void applyBlobToAwsPutObject(PutObjectRequest.Builder requestBuilder, Blob blob) {
        if (blob.encoding() != null) {
            requestBuilder.contentEncoding(blob.encoding());
        }
        Map<String, String> metadata = nonEmptyMetadata(blob.getUserMetadata());
        if (metadata != null) {
            requestBuilder.metadata(metadata);
        }
        if (blob.expires() != null) {
            requestBuilder.expires(blob.expires().toInstant(ZoneOffset.UTC));
        }
    }

    public static void applyOptionsToAwsPutObject(PutObjectRequest.Builder requestBuilder, BlobWriteOptions options) {
        if (options == null) {
            return;
        }
        if (options.encoding() != null) {
            requestBuilder.contentEncoding(options.encoding());
        }
        Map<String, String> metadata = nonEmptyMetadata(options.userMetadata());
        if (metadata != null) {
            requestBuilder.metadata(metadata);
        }
    }

    public static void applyBlobToGcpBlobInfo(BlobInfo.Builder blobBuilder, Blob blob) {
        if (blob.encoding() != null && !blob.encoding().isBlank()) {
            blobBuilder.setContentEncoding(blob.encoding());
        }
        Map<String, String> metadata = nonEmptyMetadata(blob.getUserMetadata());
        if (metadata != null) {
            blobBuilder.setMetadata(metadata);
        }
    }

    public static void applyOptionsToGcpBlobInfo(BlobInfo.Builder blobBuilder, BlobWriteOptions options) {
        if (options == null) {
            return;
        }
        if (options.encoding() != null && !options.encoding().isBlank()) {
            blobBuilder.setContentEncoding(options.encoding());
        }
        Map<String, String> metadata = nonEmptyMetadata(options.userMetadata());
        if (metadata != null) {
            blobBuilder.setMetadata(metadata);
        }
    }

    public static BlobParallelUploadOptions buildAzureUploadOptions(Blob blob) {
        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        BlobParallelUploadOptions uploadOptions = new BlobParallelUploadOptions(BinaryData.fromBytes(content));

        BlobHttpHeaders headers = toAzureHeaders(blob.encoding());
        if (headers != null) {
            uploadOptions.setHeaders(headers);
        }
        Map<String, String> metadata = nonEmptyMetadata(blob.getUserMetadata());
        if (metadata != null) {
            uploadOptions.setMetadata(metadata);
        }
        return uploadOptions;
    }

    public static BlobHttpHeaders toAzureHeaders(BlobWriteOptions options) {
        return options == null ? null : toAzureHeaders(options.encoding());
    }

    public static Map<String, String> toAzureMetadata(BlobWriteOptions options) {
        return options == null ? null : nonEmptyMetadata(options.userMetadata());
    }

    private static BlobHttpHeaders toAzureHeaders(String encoding) {
        if (encoding == null || encoding.isBlank()) {
            return null;
        }
        return new BlobHttpHeaders().setContentEncoding(encoding);
    }

    private static Map<String, String> nonEmptyMetadata(Map<String, String> metadata) {
        return (metadata == null || metadata.isEmpty()) ? null : metadata;
    }
}
