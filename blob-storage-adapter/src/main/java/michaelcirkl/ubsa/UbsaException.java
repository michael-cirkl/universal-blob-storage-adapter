package michaelcirkl.ubsa;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.cloud.storage.StorageException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Objects;

public class UbsaException extends RuntimeException {
    private final RuntimeException nativeException;

    public UbsaException(String message, S3Exception nativeException) {
        super(message, nativeException);
        this.nativeException = Objects.requireNonNull(nativeException, "nativeException must not be null");
    }

    public UbsaException(S3Exception nativeException, String message) {
        this(message, nativeException);
    }

    public UbsaException(String message, BlobStorageException nativeException) {
        super(message, nativeException);
        this.nativeException = Objects.requireNonNull(nativeException, "nativeException must not be null");
    }

    public UbsaException(BlobStorageException nativeException, String message) {
        this(message, nativeException);
    }

    public UbsaException(String message, StorageException nativeException) {
        super(message, nativeException);
        this.nativeException = Objects.requireNonNull(nativeException, "nativeException must not be null");
    }

    public UbsaException(StorageException nativeException, String message) {
        this(message, nativeException);
    }

    public RuntimeException unwrap() {
        return nativeException;
    }
}
