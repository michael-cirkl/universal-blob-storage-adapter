package michaelcirkl.ubsa.client.exception;

import com.google.cloud.storage.StorageException;

import java.util.function.Supplier;

public class GCPExceptionHandler {
    public <T> T handle(Supplier<T> action) {
        try {
            return action.get();
        } catch (StorageException error) {
            throw new UbsaException(error.getMessage(), error, error.getCode());
        }
    }
}
