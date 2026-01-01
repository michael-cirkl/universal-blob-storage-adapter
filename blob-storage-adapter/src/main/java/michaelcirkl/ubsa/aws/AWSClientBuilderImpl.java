package michaelcirkl.ubsa.aws;

import michaelcirkl.ubsa.BlobStorageClient;
import michaelcirkl.ubsa.BlobStorageClientBuilder;

public class AWSClientBuilderImpl implements BlobStorageClientBuilder {
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String region;

    @Override
    public BlobStorageClientBuilder endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    @Override
    public BlobStorageClientBuilder credentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        return this;
    }

    @Override
    public BlobStorageClientBuilder region(String region) {
        this.region = region;
        return this;
    }

    @Override
    public BlobStorageClient build() {
        return new AWSClientImpl(endpoint, accessKey, secretKey, region);
    }
}
