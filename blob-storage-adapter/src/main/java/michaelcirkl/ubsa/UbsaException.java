package michaelcirkl.ubsa;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.cloud.storage.StorageException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class UbsaException extends RuntimeException {
    private final RuntimeException nativeException;
    private final Class<? extends RuntimeException> nativeType;

    public UbsaException(String message, S3Exception nativeException) {
        this(message, nativeException, S3Exception.class);
    }

    public UbsaException(S3Exception nativeException, String message) {
        this(message, nativeException);
    }

    public UbsaException(String message, BlobStorageException nativeException) {
        this(message, nativeException, BlobStorageException.class);
    }

    public UbsaException(BlobStorageException nativeException, String message) {
        this(message, nativeException);
    }

    public UbsaException(String message, StorageException nativeException) {
        this(message, nativeException, StorageException.class);
    }

    public UbsaException(StorageException nativeException, String message) {
        this(message, nativeException);
    }

    public <T extends RuntimeException> T unwrap() {
        return (T) nativeType.cast(nativeException);
    }

    private <T extends RuntimeException> UbsaException(
            String message,
            T nativeException,
            Class<T> nativeType
    ) {
        super(message, nativeException);
        this.nativeException = nativeException;
        this.nativeType = nativeType;
    }
}
