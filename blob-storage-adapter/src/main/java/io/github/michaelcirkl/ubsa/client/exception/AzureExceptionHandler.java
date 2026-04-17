package io.github.michaelcirkl.ubsa.client.exception;

import com.azure.storage.blob.models.BlobStorageException;
import io.github.michaelcirkl.ubsa.client.exception.types.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AzureExceptionHandler {
    public <T> T handle(IOSupplier<T> action) {
        try {
            return action.get();
        } catch (Throwable error) { // here can catch all specific Azure exception types and handle them
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

    public UbsaException wrap(BlobStorageException error) {
        return new UbsaException(error.getMessage(), error, error.getStatusCode());
    }

    public UbsaException propagate(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof UbsaException ubsaException) {
            return ubsaException;
        }
        if (cause instanceof IOException ioException) {
            return new UbsaException(ioException.getMessage(), ioException);
        }
        if (cause instanceof BlobStorageException e) {
            String errorCode = e.getErrorCode() == null ? null : e.getErrorCode().toString();
            if (errorCode == null || errorCode.isBlank()) {
                return wrap(e);
            }
            switch (errorCode) {
                case "ContainerNotFound":
                    return new BucketNotFoundException(e.getMessage(), e, e.getStatusCode());
                case "BlobNotFound":
                    return new BlobNotFoundException(e.getMessage(), e, e.getStatusCode());
                case "ContainerAlreadyExists":
                    return new BucketAlreadyExistsException(e.getMessage(), e, e.getStatusCode());
                case "ConditionNotMet",
                     "AppendPositionConditionNotMet",
                     "MaxBlobSizeConditionNotMet",
                     "SequenceNumberConditionNotMet",
                     "SourceConditionNotMet",
                     "TargetConditionNotMet":
                    return new ConditionFailedException(e.getMessage(), e, e.getStatusCode());
                case "AuthenticationFailed",
                     "InvalidAuthenticationInfo",
                     "NoAuthenticationInformation":
                    return new AuthenticationFailedException(e.getMessage(), e, e.getStatusCode());
                case "AuthorizationFailure",
                     "AuthorizationPermissionMismatch",
                     "AuthorizationProtocolMismatch",
                     "AuthorizationResourceTypeMismatch",
                     "AuthorizationServiceMismatch",
                     "AuthorizationSourceIPMismatch":
                    return new AccessDeniedException(e.getMessage(), e, e.getStatusCode());
                case "OperationTimedOut":
                    return new RequestTimeoutException(e.getMessage(), e, e.getStatusCode());
                case "ConditionHeadersNotSupported",
                     "InvalidBlobOrBlock",
                     "InvalidBlobTier",
                     "InvalidBlobType",
                     "InvalidBlockId",
                     "InvalidBlockList",
                     "InvalidHeaderValue",
                     "InvalidHttpVerb",
                     "InvalidInput",
                     "InvalidMd5",
                     "InvalidMetadata",
                     "InvalidOperation",
                     "InvalidPageRange",
                     "InvalidQueryParameterValue",
                     "InvalidRange",
                     "InvalidResourceName",
                     "InvalidSourceBlobType",
                     "InvalidSourceBlobUrl",
                     "InvalidUri",
                     "InvalidVersionForPageBlobOperation",
                     "InvalidXmlDocument",
                     "InvalidXmlNodeValue",
                     "MultipleConditionHeadersNotSupported",
                     "RequestBodyTooLarge",
                     "RequestUrlFailedToParse",
                     "ResourceTypeMismatch":
                    return new InvalidRequestException(e.getMessage(), e, e.getStatusCode());
            }
            return wrap(e);
        }
        return new UbsaException(cause.getMessage(), cause);
    }

    public boolean isBucketAlreadyExists(BlobStorageException error) {
        String errorCode = error.getErrorCode() == null ? null : error.getErrorCode().toString();
        return "ContainerAlreadyExists".equals(errorCode) || error.getStatusCode() == 409;
    }

    public interface IOSupplier<T> { // basically just Supplier<T> (function with no args), but can throw IOException.
        T get() throws IOException;
    }
}
