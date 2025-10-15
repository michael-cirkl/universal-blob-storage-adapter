package michaelcirkl.ubsa.testclients;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.charset.StandardCharsets;

public class AWSTest implements BlobStoreTest {
    @Override
    public void test() {
        String bucketName = "my-local-bucket";
        String key = "example.txt";
        String content = "Hello from Java and LocalStack S3!";

        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();


        try {
            // 1. Create bucket (if not exists)
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

            // 2. Upload a "blob" (text content)
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    RequestBody.fromString(content));

            System.out.println("✅ Uploaded blob to S3 bucket.");

            // 3. Read back the blob
            var response = s3.getObject(GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build());

            String downloaded = new String(response.readAllBytes(), StandardCharsets.UTF_8);
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
