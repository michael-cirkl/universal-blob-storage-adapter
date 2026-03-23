package michaelcirkl.ubsa.client.exception.azure;

import com.azure.storage.blob.models.BlobStorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;

import java.util.function.Supplier;

public class AzureSyncExceptionHandler {
    public <T> T handle(Supplier<T> action) {
        try {
            return action.get();
        } catch (BlobStorageException error) { // here can catch all specific Azure exception types and handle them
            throw new UbsaException(error.getMessage(), error, error.getStatusCode());
        }
    }
}
