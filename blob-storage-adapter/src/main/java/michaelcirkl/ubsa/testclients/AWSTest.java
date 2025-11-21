package michaelcirkl.ubsa.testclients;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class AWSTest implements BlobStoreTest {
    @Override
    public void test() {
        String bucketName = "my-local-bucket";
        String key = "example.txt";
        String content = "Hello from Java and LocalStack S3!";

        S3AsyncClient s3 = S3AsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();


        try {
            // 1. Create bucket (if not exists)
            CompletableFuture<CreateBucketResponse> create =
                    s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            create.get();


            // 2. Upload a "blob" (text content)
            CompletableFuture<PutObjectResponse> put =
                    s3.putObject(
                            PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                            AsyncRequestBody.fromByteBuffer(ByteBuffer.wrap(content.getBytes()))
                    );
            put.get();

            System.out.println("✅ Uploaded blob to S3 bucket.");

            // 3. Read back the blob
            CompletableFuture<String> response =
                    s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(),
                                    AsyncResponseTransformer.toBytes())
                            .thenApply(r -> new String(r.asByteArray(), StandardCharsets.UTF_8));

            String downloaded = new String(response.get().getBytes(), StandardCharsets.UTF_8);
            System.out.println("📄 Blob content:");
            System.out.println(downloaded);

        } catch (S3Exception e) {
            System.err.println("❌ S3 Error: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            s3.close();
        }
    }
}
