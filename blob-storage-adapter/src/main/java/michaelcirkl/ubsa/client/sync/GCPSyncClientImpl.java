package michaelcirkl.ubsa.client.sync;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class GCPSyncClientImpl implements BlobStorageSyncClient {
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
        return client.get(bucketName) != null;
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        com.google.cloud.storage.Blob gcsBlob = getRequiredBlob(bucketName, blobKey);
        return Blob.builder()
                .bucket(bucketName)
                .key(blobKey)
                .content(gcsBlob.getContent())
                .size(gcsBlob.getSize())
                .lastModified(toLocalDateTime(gcsBlob.getUpdateTime()))
                .encoding(gcsBlob.getContentEncoding())
                .etag(gcsBlob.getEtag())
                .userMetadata(gcsBlob.getMetadata())
                .publicURI(toGsUri(bucketName, blobKey))
                .build();
    }

    @Override
    public Void deleteBucket(String bucketName) {
        client.delete(bucketName);
        return null;
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        return client.get(bucketName, blobKey) != null;
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        BlobInfo.Builder blobBuilder = BlobInfo.newBuilder(bucketName, blob.getKey());
        if (blob.encoding() != null && !blob.encoding().isBlank()) {
            blobBuilder.setContentEncoding(blob.encoding());
        }
        Map<String, String> metadata = blob.getUserMetadata();
        if (metadata != null && !metadata.isEmpty()) {
            blobBuilder.setMetadata(metadata);
        }

        byte[] content = blob.getContent() == null ? new byte[0] : blob.getContent();
        com.google.cloud.storage.Blob created = client.create(blobBuilder.build(), content);
        return created.getEtag();
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        client.delete(bucketName, blobKey);
        return null;
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        CopyRequest request = CopyRequest.newBuilder()
                .setSource(BlobId.of(sourceBucketName, sourceBlobKey))
                .setTarget(BlobId.of(destinationBucketName, destinationBlobKey))
                .build();
        CopyWriter copyWriter = client.copy(request);
        com.google.cloud.storage.Blob copied = copyWriter.getResult();
        return copied.getEtag();
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        Set<Bucket> buckets = new HashSet<>();
        Page<com.google.cloud.storage.Bucket> bucketPage = client.list(BucketListOption.pageSize(1000));
        bucketPage.iterateAll().forEach(gcsBucket -> {
            LocalDateTime created = toLocalDateTime(gcsBucket.getCreateTime());
            buckets.add(Bucket.builder()
                    .name(gcsBucket.getName())
                    .publicURI(toGsUri(gcsBucket.getName(), null))
                    .creationDate(created)
                    .lastModified(created)
                    .build());
        });
        return buckets;
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        Page<com.google.cloud.storage.Blob> blobPage = (prefix != null && !prefix.isBlank())
                ? client.list(bucketName, BlobListOption.prefix(prefix))
                : client.list(bucketName);
        return mapBlobsFromPage(bucketName, blobPage);
    }

    @Override
    public Void createBucket(Bucket bucket) {
        client.create(BucketInfo.of(bucket.getName()));
        return null;
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        return listBlobsByPrefix(bucketName, null);
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        client.delete(bucketName);
        return null;
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        validateRange(startInclusive, endInclusive);
        long requestedLength = endInclusive - startInclusive + 1;
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
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read byte range from GCS blob.", e);
        }
    }

    @Override
    public String createBlobIfNotExists(String bucketName, Blob blob) {
        com.google.cloud.storage.Blob existing = client.get(bucketName, blob.getKey());
        if (existing != null) {
            return existing.getEtag();
        }
        return createBlob(bucketName, blob);
    }

    @Override
    public URL generateGetUrl(String bucket, String objectKey, Duration expiry) {
        long seconds = toPositiveSeconds(expiry);
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, objectKey).build();
        return client.signUrl(blobInfo, seconds, TimeUnit.SECONDS);
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
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
    }

    private static long toPositiveSeconds(Duration expiry) {
        if (expiry == null || expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("Expiry must be a positive duration.");
        }
        return expiry.toSeconds();
    }

    private static void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private static URI toGsUri(String bucketName, String objectKey) {
        String uri = (objectKey == null || objectKey.isBlank())
                ? "gs://" + bucketName
                : "gs://" + bucketName + "/" + objectKey;
        return URI.create(uri);
    }

    private static LocalDateTime toLocalDateTime(Long epochMilli) {
        if (epochMilli == null) {
            return null;
        }
        return Instant.ofEpochMilli(epochMilli).atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    private Set<Blob> mapBlobsFromPage(String bucketName, Page<com.google.cloud.storage.Blob> blobPage) {
        Set<Blob> blobs = new HashSet<>();
        blobPage.iterateAll().forEach(gcsBlob -> blobs.add(Blob.builder()
                .bucket(bucketName)
                .key(gcsBlob.getName())
                .size(gcsBlob.getSize())
                .lastModified(toLocalDateTime(gcsBlob.getUpdateTime()))
                .encoding(gcsBlob.getContentEncoding())
                .etag(gcsBlob.getEtag())
                .userMetadata(gcsBlob.getMetadata())
                .publicURI(toGsUri(bucketName, gcsBlob.getName()))
                .build()));
        return blobs;
    }

    private com.google.cloud.storage.Blob getRequiredBlob(String bucketName, String blobKey) {
        com.google.cloud.storage.Blob blob = client.get(bucketName, blobKey);
        if (blob == null) {
            throw new IllegalStateException("Blob not found: gs://" + bucketName + "/" + blobKey);
        }
        return blob;
    }
}
