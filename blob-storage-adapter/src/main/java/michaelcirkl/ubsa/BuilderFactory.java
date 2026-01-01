package michaelcirkl.ubsa;

import michaelcirkl.ubsa.aws.AWSClientBuilderImpl;
import michaelcirkl.ubsa.azure.AzureClientBuilderImpl;
import michaelcirkl.ubsa.gcp.GCPClientBuilderImpl;

public class BuilderFactory {
    private static Provider currentProvider;
    public static BlobStorageClientBuilder getClientBuilder() {
        String SDKType = System.getProperty("blobstorage.provider");
        if ("AWS".equalsIgnoreCase(SDKType)) {
            currentProvider = Provider.AWS;
            return new AWSClientBuilderImpl();
        } else if ("Azure".equalsIgnoreCase(SDKType)) {
            currentProvider = Provider.Azure;
            return new AzureClientBuilderImpl();
        } else if ("GCP".equalsIgnoreCase(SDKType)) {
            currentProvider = Provider.GCP;
            return new GCPClientBuilderImpl();
        }

        throw new IllegalArgumentException(String.format("Invalid Blobstorage provider: %s. Pass blobstorage.provider as a VM option.", SDKType));
    }

    public static Provider getCurrentProvider() {
        return currentProvider;
    }
}
