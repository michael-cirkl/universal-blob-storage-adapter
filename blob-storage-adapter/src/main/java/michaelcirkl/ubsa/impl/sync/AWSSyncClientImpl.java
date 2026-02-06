package michaelcirkl.ubsa.impl.sync;

import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageSyncClient;
import michaelcirkl.ubsa.Bucket;
import michaelcirkl.ubsa.Provider;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Set;

public class AWSSyncClientImpl implements BlobStorageSyncClient {
    private final S3Client s3Client;

    public AWSSyncClientImpl(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public Provider getProvider() {
        return Provider.AWS;
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
}
