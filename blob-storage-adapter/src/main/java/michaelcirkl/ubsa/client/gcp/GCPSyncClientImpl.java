package michaelcirkl.ubsa.client.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.CopyRequest;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.*;
import michaelcirkl.ubsa.client.exception.GCPExceptionHandler;
import michaelcirkl.ubsa.client.pagination.BucketListingSupport;
import michaelcirkl.ubsa.client.pagination.ListingPage;
import michaelcirkl.ubsa.client.pagination.PageRequest;
import michaelcirkl.ubsa.client.streaming.BlobWriteOptions;
import michaelcirkl.ubsa.client.streaming.ContentLengthValidators;
import michaelcirkl.ubsa.client.streaming.FileUploadValidators;
import michaelcirkl.ubsa.client.streaming.WriteOptionsMappers;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.channels.Channels;
import java.time.Duration;
import java.util.List;

public class GCPSyncClientImpl implements BlobStorageSyncClient {
    private final GCPExceptionHandler exceptionHandler = new GCPExceptionHandler();
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
        return exceptionHandler.handle(() -> {
            com.google.cloud.storage.Blob blob = requireBlob(bucketName, blobKey);
            return GCPClientSupport.mapFetchedBlob(bucketName, blobKey, blob, blob.getContent());
        });
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
    public ListingPage<Bucket> listBuckets(PageRequest request) {
        PageRequest pageRequest = GCPClientSupport.normalizePageRequest(request);
        return exceptionHandler.handle(() -> {
            Page<com.google.cloud.storage.Bucket> bucketPage = client.list(GCPClientSupport.buildBucketListOptions(pageRequest));
            return ListingPage.of(GCPClientSupport.mapBuckets(bucketPage.getValues()), bucketPage.getNextPageToken());
        });
    }

    @Override
    public ListingPage<Blob> listBlobs(String bucketName, String prefix, PageRequest request) {
        PageRequest pageRequest = GCPClientSupport.normalizePageRequest(request);
        return exceptionHandler.handle(() -> {
            Page<com.google.cloud.storage.Blob> blobPage = client.list(bucketName, GCPClientSupport.buildBlobListOptions(prefix, pageRequest));
            return ListingPage.of(GCPClientSupport.mapBlobsFromPage(bucketName, blobPage.getValues()), blobPage.getNextPageToken());
        });
    }

    @Override
    public List<Bucket> listAllBuckets() {
        return BucketListingSupport.listAllBuckets(this::listBuckets);
    }

    @Override
    public Void createBucket(Bucket bucket) {
        return exceptionHandler.handle(() -> {
            client.create(BucketInfo.of(bucket.getName()));
            return null;
        });
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
        return exceptionHandler.handle(() -> GCPClientSupport.generateGetUrl(client, bucket, objectKey, expiry));
    }

    @Override
    public URL generatePutUrl(String bucket, String objectKey, Duration expiry, String contentType) {
        return exceptionHandler.handle(() -> GCPClientSupport.generatePutUrl(client, bucket, objectKey, expiry, contentType));
    }

    private void validateRange(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }
    }

    private com.google.cloud.storage.Blob requireBlob(String bucketName, String blobKey) {
        com.google.cloud.storage.Blob blob = client.get(bucketName, blobKey);
        if (blob == null) {
            throw new StorageException(404, "Blob not found: gs://" + bucketName + "/" + blobKey);
        }
        return blob;
    }
}
