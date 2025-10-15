package michaelcirkl.ubsa;

import michaelcirkl.ubsa.testclients.AWSTest;
import michaelcirkl.ubsa.testclients.AzureTest;
import michaelcirkl.ubsa.testclients.BlobStoreTest;
import michaelcirkl.ubsa.testclients.GCPTest;

public class WrapperFactory {
    public static BlobStoreTest getWrapper() {
        String SDKType = System.getProperty("blobstorage.provider");

        if ("AWS".equalsIgnoreCase(SDKType)) {
            return new AWSTest();
        } else if ("Azure".equalsIgnoreCase(SDKType)) {
            return new AzureTest();
        } else if ("GCP".equalsIgnoreCase(SDKType)) {
            return new GCPTest();
        }

        throw new IllegalArgumentException(String.format("Invalid Blobstorage provider: %s. Pass blobstorage.provider as a VM option.", SDKType));
    }
}
