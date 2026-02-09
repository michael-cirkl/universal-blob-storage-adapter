package org.example;


import michaelcirkl.ubsa.Blob;
import michaelcirkl.ubsa.BlobStorageAsyncClient;
import michaelcirkl.ubsa.BlobStorageClientFactory;
import michaelcirkl.ubsa.Bucket;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreatePolicyResponse;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeletePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DetachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class AwsAsyncMain {
    private record RoleSetup(
            String roleArn,
            String policyArn
    ) {}

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        URI endpoint = URI.create(System.getenv().getOrDefault("LOCALSTACK_ENDPOINT", "http://localhost:4566"));
        Region region = Region.US_EAST_1;
        AwsBasicCredentials bootstrapCredentials = AwsBasicCredentials.create("test", "test");

        String suffix = String.valueOf(ThreadLocalRandom.current().nextInt(100000, 999999));
        String bucketName = "mybucket-" + suffix;
        String roleName = "ubsa-iam-role-" + suffix;
        String policyName = "ubsa-iam-policy-" + suffix;
        String roleSessionName = "ubsa-session-" + suffix;

        RoleSetup roleSetup = createRoleSetup(
                endpoint,
                region,
                bootstrapCredentials,
                roleName,
                policyName,
                bucketName
        );
        AwsSessionCredentials roleCredentials = assumeRoleCredentials(
                endpoint,
                region,
                bootstrapCredentials,
                roleSetup.roleArn(),
                roleSessionName
        );

        try (S3AsyncClient s3 = S3AsyncClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        roleCredentials))
                .build()) {
            BlobStorageAsyncClient client = BlobStorageClientFactory.getAsyncClient(s3);
            System.out.println("Running provider: " + client.getProvider());

            client.createBucket(Bucket.builder().name(bucketName).build()).get();
            System.out.println("Bucket exists: " + client.bucketExists(bucketName).get());

            Blob blob = Blob.builder()
                    .content("hello from AWS IAM user".getBytes(StandardCharsets.UTF_8))
                    .bucket(bucketName)
                    .key("example")
                    .build();
            client.createBlob(bucketName, blob).get();
            System.out.println("Blob exists: " + client.blobExists(bucketName, "example").get());

            byte[] content = client.getBlob(bucketName, "example").get().getContent();
            System.out.println("Blob content: " + new String(content, StandardCharsets.UTF_8));

            URL presignedPut = client.generatePutUrl(bucketName, "example", Duration.ofMinutes(10), "text/plain").get();
            URL presignedGet = client.generateGetUrl(bucketName, "example", Duration.ofMinutes(10)).get();
            System.out.println("Presigned PUT URL: " + presignedPut);
            System.out.println("Presigned GET URL: " + presignedGet);

            client.deleteBlob(bucketName, "example").get();
            client.deleteBucket(bucketName).get();
            System.out.println("IAM role auth test passed with temporary STS credentials.");
        } finally {
            cleanupRoleResources(endpoint, region, bootstrapCredentials, roleName, roleSetup.policyArn());
        }
    }

    private static RoleSetup createRoleSetup(
            URI endpoint,
            Region region,
            AwsBasicCredentials bootstrapCredentials,
            String roleName,
            String policyName,
            String bucketName
    ) {
        try (IamClient iam = IamClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(bootstrapCredentials))
                .build()) {
            String accountId = getAccountId(endpoint, region, bootstrapCredentials);
            String trustPolicyDocument = "{"
                    + "\"Version\":\"2012-10-17\","
                    + "\"Statement\":["
                    + "{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::" + accountId + ":root\"},\"Action\":\"sts:AssumeRole\"}"
                    + "]"
                    + "}";

            CreateRoleResponse createdRole = iam.createRole(builder -> builder
                    .roleName(roleName)
                    .assumeRolePolicyDocument(trustPolicyDocument));
            System.out.println("Created IAM role: " + createdRole.role().roleName());

            String policyDocument = "{"
                    + "\"Version\":\"2012-10-17\","
                    + "\"Statement\":["
                    + "{\"Effect\":\"Allow\",\"Action\":[\"s3:CreateBucket\",\"s3:ListBucket\",\"s3:DeleteBucket\"],\"Resource\":[\"arn:aws:s3:::" + bucketName + "\"]},"
                    + "{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:GetObject\",\"s3:DeleteObject\",\"s3:HeadObject\"],\"Resource\":[\"arn:aws:s3:::" + bucketName + "/*\"]}"
                    + "]"
                    + "}";
            CreatePolicyResponse createdPolicy = iam.createPolicy(builder -> builder
                    .policyName(policyName)
                    .policyDocument(policyDocument));
            String policyArn = createdPolicy.policy().arn();

            iam.attachRolePolicy(AttachRolePolicyRequest.builder()
                    .roleName(roleName)
                    .policyArn(policyArn)
                    .build());
            return new RoleSetup(createdRole.role().arn(), policyArn);
        } catch (IamException iamException) {
            if (iamException.statusCode() == 501) {
                throw new IllegalStateException(
                        "IAM is not enabled on LocalStack endpoint " + endpoint
                                + ". Start LocalStack with SERVICES=s3,iam,sts.",
                        iamException
                );
            }
            throw iamException;
        }
    }

    private static AwsSessionCredentials assumeRoleCredentials(
            URI endpoint,
            Region region,
            AwsBasicCredentials bootstrapCredentials,
            String roleArn,
            String roleSessionName
    ) {
        try (StsClient sts = StsClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(bootstrapCredentials))
                .build()) {
            AssumeRoleResponse response = sts.assumeRole(AssumeRoleRequest.builder()
                    .roleArn(roleArn)
                    .roleSessionName(roleSessionName)
                    .build());
            System.out.println("Assumed IAM role: " + roleArn);
            return AwsSessionCredentials.create(
                    response.credentials().accessKeyId(),
                    response.credentials().secretAccessKey(),
                    response.credentials().sessionToken()
            );
        }
    }

    private static String getAccountId(
            URI endpoint,
            Region region,
            AwsBasicCredentials bootstrapCredentials
    ) {
        try (StsClient sts = StsClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(bootstrapCredentials))
                .build()) {
            GetCallerIdentityResponse identity = sts.getCallerIdentity();
            return identity.account();
        }
    }

    private static void cleanupRoleResources(
            URI endpoint,
            Region region,
            AwsBasicCredentials bootstrapCredentials,
            String roleName,
            String policyArn
    ) {
        try (IamClient iam = IamClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(bootstrapCredentials))
                .build()) {
            if (policyArn != null) {
                iam.detachRolePolicy(DetachRolePolicyRequest.builder()
                        .roleName(roleName)
                        .policyArn(policyArn)
                        .build());
                iam.deletePolicy(DeletePolicyRequest.builder()
                        .policyArn(policyArn)
                        .build());
            }
            iam.deleteRole(DeleteRoleRequest.builder()
                    .roleName(roleName)
                    .build());
        } catch (Exception cleanupError) {
            System.err.println("Cleanup warning: " + cleanupError.getMessage());
        }
    }
}
