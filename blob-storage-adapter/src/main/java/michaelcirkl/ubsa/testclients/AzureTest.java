package michaelcirkl.ubsa.testclients;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.*;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class AzureTest implements BlobStoreTest {
    @Override
    public void test() {
        String accountName = "devstoreaccount1";
        String accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        String blobName = "hello.txt";
        String content = "Hello from Java to Azurite!";
        String containerName = "my-container";

        String endpoint = String.format("http://127.0.0.1:10000/%s", accountName);

        // 2. Auth
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceAsyncClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildAsyncClient();

        // 3. Create container (if not exists)
        BlobContainerAsyncClient blobContainerAsyncClient = blobServiceClient.getBlobContainerAsyncClient(containerName);

        if (Boolean.FALSE.equals(blobContainerAsyncClient.exists().block())) {
            blobContainerAsyncClient = blobServiceClient.createBlobContainer(containerName).block();
        }

        // 4. Upload blob
        BlobAsyncClient blobAsyncClient = blobContainerAsyncClient.getBlobAsyncClient(blobName);

        blobAsyncClient.upload(BinaryData.fromBytes(content.getBytes()), true).block();

        // 5. Download blob
        BinaryData binaryData = blobAsyncClient.downloadContent().block();
        System.out.println("Downloaded content: " + binaryData);

    }
}
