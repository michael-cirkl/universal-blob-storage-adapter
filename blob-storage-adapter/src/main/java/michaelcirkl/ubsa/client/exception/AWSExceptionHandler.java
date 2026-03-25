package michaelcirkl.ubsa.client.exception;

import michaelcirkl.ubsa.client.exception.types.*;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

public final class AWSExceptionHandler {
    public <T> T handle(Supplier<T> action) {
        try {
            return action.get();
        } catch (S3Exception error) { // here can catch all specific S3 exception types and handle them
            throw propagate(error);
        }
    }

    public <T> CompletableFuture<T> handleAsync(CompletableFuture<T> future) {
        return future.handle((result, error) -> {
            if (error == null) {
                return result;
            }
            throw propagate(error);
        });
    }

    public Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException completionException && completionException.getCause() != null) {
            return completionException.getCause();
        }
        return error;
    }

    public UbsaException wrap(S3Exception error) {
        return new UbsaException(error.getMessage(), error, error.statusCode());
    }

    public RuntimeException propagate(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof UbsaException ubsaException) {
            return ubsaException;
        }
        if (cause instanceof IOException ioException) {
            return new UbsaException(ioException.getMessage(), ioException);
        }
        if (cause instanceof S3Exception e) {
            switch (e.awsErrorDetails().errorCode()) {
                case "NoSuchBucket":
                    return new BucketNotFoundException(e.getMessage(), e, e.statusCode());
                case "NoSuchKey":
                    return new BlobNotFoundException(e.getMessage(), e, e.statusCode());
                case "BucketAlreadyExists", "BucketAlreadyOwnedByYou":
                    return new BucketAlreadyExistsException(e.getMessage(), e, e.statusCode());
                case "BucketNotEmpty":
                    return new BucketNotEmptyException(e.getMessage(), e, e.statusCode());
                case "PreconditionFailed":
                    return new ConditionFailedException(e.getMessage(), e, e.statusCode());
                case "InvalidToken", "ExpiredToken":
                    return new AuthenticationFailedException(e.getMessage(), e, e.statusCode());
                case "AccessDenied":
                    return new AccessDeniedException(e.getMessage(), e, e.statusCode());
                case "RequestTimeout":
                    return new RequestTimeoutException(e.getMessage(), e, e.statusCode());
                case    "InvalidArgument",
                        "InvalidBucketName",
                        "InvalidRange",
                        "InvalidDigest",
                        "BadDigest",
                        "InvalidRequest",
                        "InvalidStorageClass",
                        "InvalidURI":
                    return new InvalidRequestException(e.getMessage(), e, e.statusCode());
            }
            return wrap(e);
        }
        return new UbsaException(cause.getMessage(), cause);
    }

    public boolean isNotFound(S3Exception error) {
        if (error instanceof NoSuchBucketException || error instanceof NoSuchKeyException) {
            return true;
        }
        return error.statusCode() == 404;
    }
}
