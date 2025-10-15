package michaelcirkl.ubsa.testclients;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;

public class GCPTest implements BlobStoreTest {
    @Override
    public void test() {
        String emulatorHost = "http://localhost:9023";

        // Dummy project ID (emulator doesn’t enforce)
        String projectId = "demo-project";

        // Create the Storage client with explicit host + no credentials
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(projectId)
                .setHost(emulatorHost)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();

        String bucketName = "test-bucket";
        String blobName = "hello.txt";
        String content = "Hello from Java using REST client and GCP emulator!";

        // 1. Create bucket (if doesn't exist)
        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            System.out.println("Creating bucket: " + bucketName);
            bucket = storage.create(BucketInfo.of(bucketName));
        }

        // 2. Upload blob
        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobName)
                .setContentType("text/plain")
                .build();
        storage.create(blobInfo, content.getBytes());
        System.out.println("Uploaded blob: " + blobName);

        // 3. Download blob
        byte[] downloaded = storage.readAllBytes(bucketName, blobName);
        System.out.println("Downloaded content: " + new String(downloaded));
    }
}
