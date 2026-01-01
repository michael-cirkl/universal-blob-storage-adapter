package michaelcirkl.ubsa;

public interface BlobStorageClientBuilder {
    BlobStorageClientBuilder endpoint(String endpoint);
    BlobStorageClientBuilder credentials(String accessKey, String secretKey);
    BlobStorageClientBuilder region(String region);
    BlobStorageClient build();
}
