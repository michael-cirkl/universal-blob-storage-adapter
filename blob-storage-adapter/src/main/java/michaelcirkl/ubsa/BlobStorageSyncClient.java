package michaelcirkl.ubsa;

import java.util.Set;

public interface BlobStorageSyncClient {
    Provider getProvider();

    Boolean bucketExists(String bucketName);

    Blob getBlob(String bucketName, String blobKey);

    Void deleteBucket(String bucketName);

    Boolean blobExists(String bucketName, String blobKey);

    String createBlob(String bucketName, Blob blob);

    Void deleteBlob(String bucketName, String blobKey);

    String copyBlob(
            String sourceBucketName,
            String sourceBlobKey,
            String destinationBucketName,
            String destinationBlobKey
    );

    Set<Bucket> listAllBuckets();

    Set<Blob> listBlobsByPrefix(String bucketName, String prefix);

    Void createBucket(Bucket bucket);

    Set<Blob> getAllBlobsInBucket(String bucketName);

    Void deleteBucketIfExists(String bucketName);

    byte[] getByteRange(String bucketName, String blobKey, long startInclusive, long endInclusive);

    String createBlobIfNotExists(String bucketName, Blob blob);
}