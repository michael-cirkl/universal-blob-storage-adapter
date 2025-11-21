package michaelcirkl.ubsa.testclients;

import com.google.api.core.ApiFuture;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutionException;

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

        String bucketName = "test-bucket2";
        String blobName = "hello2.txt";
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

        try {
            BlobWriteSession writeSession = storage.blobWriteSession(blobInfo);
            WritableByteChannel channel = writeSession.open();
            channel.write(ByteBuffer.wrap(content.getBytes()));
            channel.close();  // finalizes upload
            ApiFuture<BlobInfo> result = writeSession.getResult();
            BlobInfo created = result.get();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

/*
        // 3. Download blob
        try {
            ApiFuture<BlobReadSession> future = storage.blobReadSession(blobInfo.getBlobId());
            ApiFuture<ZeroCopySupport.DisposableByteString> read = future.get().readAs(ReadProjectionConfigs.asFutureByteString());
            System.out.println(read.get().byteString().toString());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
*/
        byte[] downloaded = storage.readAllBytes(bucketName, blobName);
        System.out.println("Downloaded content: " + new String(downloaded));
    }
}
