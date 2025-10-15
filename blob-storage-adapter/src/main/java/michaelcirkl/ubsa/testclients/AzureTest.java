package michaelcirkl.ubsa.testclients;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
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

        String endpoint = String.format("http://127.0.0.1:10000/%s", accountName);

        // 2. Auth
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        // 3. Create container (if not exists)
        String containerName = "my-container";
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!containerClient.exists()) {
            containerClient.create();
        }

        // 4. Upload blob
        String blobName = "hello.txt";
        String content = "Hello from Java to Azurite!";
        InputStream dataStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.upload(dataStream, content.length(), true);  // true = overwrite

        // 5. Download blob
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blobClient.download(outputStream);
        String downloadedContent = outputStream.toString(StandardCharsets.UTF_8);

        System.out.println("Downloaded content: " + downloadedContent);
    }
}
