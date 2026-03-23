package michaelcirkl.ubsa.client.sync;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.exception.gcp.GCPSyncExceptionHandler;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;
import michaelcirkl.ubsa.client.streaming.FileUploadValidators;
import michaelcirkl.ubsa.client.streaming.WriteOptionsMappers;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GCPSyncClientImpl implements BlobStorageSyncClient {
    private final GCPSyncExceptionHandler exceptionHandler = new GCPSyncExceptionHandler();
    private final Storage client;

    public GCPSyncClientImpl(Storage client) {
        this.client = client;
    }

    @Override
    public Provider getProvider() {
        return Provider.GCP;
    }

    @Override
    public <T> T unwrap(Class<T> nativeType) {
        if (nativeType == null) {
            throw new IllegalArgumentException("Class type to unwrap must not be null.");
        }
        return nativeType.isInstance(client) ? nativeType.cast(client) : null;
    }

    @Override
    public Boolean bucketExists(String bucketName) {
        return exceptionHandler.handle(() -> client.get(bucketName) != null);
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> mapBlob(bucketName, blobKey, requireBlob(bucketName, blobKey)));
    }

    @Override
    public InputStream openBlobStream(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            requireBlob(bucketName, blobKey);
            ReadChannel readChannel = client.reader(BlobId.of(bucketName, blobKey));
            return Channels.newInputStream(readChannel);
        });
    }

    @Override
    public Void deleteBucket(String bucketName) {
        return exceptionHandler.handle(() -> {
            if (!client.delete(bucketName)) { // returns false if not found, other SDKs throw exception
                throw new StorageException(404, "Bucket not found: " + bucketName);
            }
            return null;
        });
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> client.get(bucketName, blobKey) != null);
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        return exceptionHandler.handle(() -> {
            BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blob.getKey());
            WriteOptionsMappers.applyBlobToGcpBlobInfo(blobBuilder, blob);

            byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
            BlobInfo blobInfo = blobBuilder.build();
            try (WriteChannel writeChannel = client.writer(blobInfo)) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) {
                    writeChannel.write(buffer);
                }
            }
            return requireBlob(bucketName, blob.getKey()).getEtag();
        });
    }

    @Override
    public String createBlob(String bucketName, String blobKey, Path sourceFile) {
        FileUploadValidators.validateSourceFile(sourceFile);
        return exceptionHandler.handle(() -> {
            BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build();
            return client.createFrom(blobInfo, sourceFile).getEtag();
        });
    }

    @Override
    public String createBlob(String bucketName, String blobKey, InputStream content, long contentLength, BlobWriteOptions options) {
        ContentLengthValidators.validateContentLength(contentLength);
        if (content == null) {
            throw new IllegalArgumentException("Content stream must not be null.");
        }
        return exceptionHandler.handle(() -> {
            BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blobKey);
            WriteOptionsMappers.applyOptionsToGcpBlobInfo(blobBuilder, options);
            BlobInfo blobInfo = blobBuilder.build();
            try (WriteChannel writeChannel = client.writer(blobInfo)) {
                ContentLengthValidators.copyInputStreamToChannel(content, writeChannel, contentLength);
            }
            return requireBlob(bucketName, blobKey).getEtag();
        });
    }

    @Override
    public Void deleteBlobIfExists(String bucketName, String blobKey) {
        return exceptionHandler.handle(() -> {
            client.delete(bucketName, blobKey);
            return null;
        });
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return exceptionHandler.handle(() -> {
            CopyRequest request = CopyRequest.newBuilder()
                    .setSource(BlobId.of(sourceBucketName, sourceBlobKey))
                    .setTarget(BlobId.of(destinationBucketName, destinationBlobKey))
                    .build();
            CopyWriter copyWriter = client.copy(request);
            com.google.cloud.storage.Blob copied = copyWriter.getResult();
            return copied.getEtag();
        });
    }

    @Override
    public List<Bucket> listAllBuckets() {
        return exceptionHandler.handle(() -> {
            List<Bucket> buckets = new ArrayList<>();
            Page<com.google.cloud.storage.Bucket> bucketPage = client.list(BucketListOption.pageSize(500));
            bucketPage.iterateAll().forEach(gcsBucket -> {
                LocalDateTime created = toLocalDateTime(gcsBucket.getCreateTimeOffsetDateTime());
                buckets.add(Bucket.builder()
                        .name(gcsBucket.getName())
                        .publicURI(toGsUri(gcsBucket.getName(), null))
                        .creationDate(created)
                        .lastModified(created)
                        .build());
            });
            return buckets;
        });
    }

    @Override
    public List<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        return exceptionHandler.handle(() -> {
            Page<com.google.cloud.storage.Blob> blobPage = (prefix != null && !prefix.isBlank())
                    ? client.list(bucketName, BlobListOption.prefix(prefix))
                    : client.list(bucketName);
            return mapBlobsFromPage(bucketName, blobPage);
        });
    }

    @Override
    public Void createBucket(Bucket bucket) {
        return exceptionHandler.handle(() -> {
            client.create(BucketInfo.of(bucket.getName()));
            return null;
        });
    }

    @Override
    public List<Blob> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        return exceptionHandler.handle(() -> {
            try {
                client.delete(bucketName);
            } catch (StorageException error) {
                if (error.getCode() != 404) { // delete if not exists means I don't throw when not found
                    throw error;
                }
            }
            return null;
        });
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        long requestedLength = endInclusive - startInclusive + 1;
        return exceptionHandler.handle(() -> {
            try (ReadChannel readChannel = client.reader(BlobId.of(bucketName, blobKey));
                 ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                readChannel.seek(startInclusive);

                long remaining = requestedLength;
                while (remaining > 0) {
                    int chunkSize = (int) Math.min(8192, remaining);
                    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
                    int read = readChannel.read(buffer);
                    if (read <= 0) {
                        break;
                    }
                    output.write(buffer.array(), 0, read);
                    remaining -= read;
                }
                return output.toByteArray();
            }
        });
    }



    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        return exceptionHandler.handle(() -> {
            long seconds = toPositiveSeconds(expiry);
            BlobInfo blobInfo = BlobInfo.newBuilder(bucket, objectKey).build();
            return client.signUrl(blobInfo, seconds, TimeUnit.SECONDS);
        });
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        return exceptionHandler.handle(() -> {
            long seconds = toPositiveSeconds(expiry);
            BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucket, objectKey);
            if (contentType != null && !contentType.isBlank()) {
                blobBuilder.setContentType(contentType);
            }
            BlobInfo blobInfo = blobBuilder.build();
            var options = new ArrayList<Storage.SignUrlOption>();
            options.add(Storage.SignUrlOption.httpMethod(HttpMethod.PUT));
            if (contentType != null && !contentType.isBlank()) {
                options.add(Storage.SignUrlOption.withContentType());
            }
            return client.signUrl(blobInfo, seconds, TimeUnit.SECONDS, options.toArray(new Storage.SignUrlOption[0]));
        });
    }

    private long toPositiveSeconds(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
        return expiry.toSeconds();
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private URI toGsUri(String bucketName, String objectKey) {
        String uri = (objectKey == null || objectKey.isBlank())
                ? "gs://" + bucketName
                : "gs://" + bucketName + "/" + objectKey;
        return URI.create(uri);
    }

    private Blob mapBlob(String bucketName, String blobKey, com.google.cloud.storage.Blob gcsBlob) {
        return Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .content(gcsBlob.getContent())
                .size(gcsBlob.getSize())
                .lastModified(toLocalDateTime(gcsBlob.getUpdateTimeOffsetDateTime()))
                .encoding(gcsBlob.getContentEncoding())
                .etag(gcsBlob.getEtag())
                .userMetadata(gcsBlob.getMetadata())
                .publicURI(toGsUri(bucketName, blobKey))
                .build();
    }

    private com.google.cloud.storage.Blob requireBlob(String bucketName, String blobKey) {
        com.google.cloud.storage.Blob blob = client.get(bucketName, blobKey);
        if (blob == null) {
            throw new StorageException(404, "Blob not found: gs://" + bucketName + "/" + blobKey);
        }
        return blob;
    }

    private LocalDateTime toLocalDateTime(OffsetDateTime time) {
        return time == null ? null : time.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private List<Blob> mapBlobsFromPage(String bucketName, Page<com.google.cloud.storage.Blob> blobPage) {
        List<Blob> blobs = new ArrayList<>();
        blobPage.iterateAll().forEach(gcsBlob -> blobs.add(Blob.builder()
                .bucket(bucketName)
                .key(gcsBlob.getName())
                .size(gcsBlob.getSize())
                .lastModified(toLocalDateTime(gcsBlob.getUpdateTimeOffsetDateTime()))
                .encoding(gcsBlob.getContentEncoding())
                .etag(gcsBlob.getEtag())
                .userMetadata(gcsBlob.getMetadata())
                .publicURI(toGsUri(bucketName, gcsBlob.getName()))
                .build()));
        return blobs;
    }

}
