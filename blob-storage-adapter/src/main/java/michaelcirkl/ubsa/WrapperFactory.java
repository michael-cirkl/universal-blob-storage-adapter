package michaelcirkl.ubsa;

import michaelcirkl.ubsa.testclients.AWSTest;
import michaelcirkl.ubsa.testclients.AzureTest;
import michaelcirkl.ubsa.testclients.BlobStoreTest;
import michaelcirkl.ubsa.testclients.GCPTest;

public class WrapperFactory {
    private static Provider currentProvider;
    public static BlobStoreTest getWrapper() {
        String SDKType = System.getProperty("blobstorage.provider");
        if ("AWS".equalsIgnoreCase(SDKType)) {
            currentProvider = Provider.AWS;
            return new AWSTest();
        } else if ("Azure".equalsIgnoreCase(SDKType)) {
            currentProvider = Provider.Azure;
            return new AzureTest();
        } else if ("GCP".equalsIgnoreCase(SDKType)) {
            currentProvider = Provider.GCP;
            return new GCPTest();
        }

        throw new IllegalArgumentException(String.format("Invalid Blobstorage provider: %s. Pass blobstorage.provider as a VM option.", SDKType));
    }

    public static Provider getCurrentProvider() {
        return currentProvider;
    }
}
