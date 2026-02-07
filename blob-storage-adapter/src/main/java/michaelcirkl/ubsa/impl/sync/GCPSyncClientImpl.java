package michaelcirkl.ubsa.impl.sync;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
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
    public Boolean bucketExists(String bucketName) {
        return null;
    }

    @Override
    public Blob getBlob(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public Void deleteBucket(String bucketName) {
        return null;
    }

    @Override
    public Boolean blobExists(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public String createBlob(String bucketName, Blob blob) {
        return null;
    }

    @Override
    public Void deleteBlob(String bucketName, String blobKey) {
        return null;
    }

    @Override
    public String copyBlob(String sourceBucketName, String sourceBlobKey, String destinationBucketName, String destinationBlobKey) {
        return null;
    }

    @Override
    public Set<Bucket> listAllBuckets() {
        return null;
    }

    @Override
    public Set<Blob> listBlobsByPrefix(String bucketName, String prefix) {
        return null;
    }

    @Override
    public Void createBucket(Bucket bucket) {
        return null;
    }

    @Override
    public Set<Blob> getAllBlobsInBucket(String bucketName) {
        return null;
    }

    @Override
    public Void deleteBucketIfExists(String bucketName) {
        return null;
    }

    @Override
    public byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive) {
        return new byte[0];
    }

    @Override
    public String createBlobIfNotExists(String bucketName, Blob blob) {
        return null;
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
}
