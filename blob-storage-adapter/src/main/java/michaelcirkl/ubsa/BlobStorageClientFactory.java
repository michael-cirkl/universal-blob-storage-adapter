package michaelcirkl.ubsa;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.google.cloud.storage.Storage;
import michaelcirkl.ubsa.impl.AWSClientImpl;
import michaelcirkl.ubsa.impl.AzureClientImpl;
import michaelcirkl.ubsa.impl.GCPClientImpl;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class BlobStorageClientFactory {
    private static Provider currentProvider;

    public static BlobStorageClient getClient(S3AsyncClient s3Client) {
        currentProvider = Provider.AWS;
        return new AWSClientImpl(s3Client);
    }

    public static BlobStorageClient getClient(BlobServiceAsyncClient azureClient) {
        currentProvider = Provider.Azure;
        return new AzureClientImpl(azureClient);
    }

    public static BlobStorageClient getClient(Storage gcpClient) {
        currentProvider = Provider.GCP;
        return new GCPClientImpl(gcpClient);
    }

    public static Provider getCurrentProvider() {
        return currentProvider;
    }
}
