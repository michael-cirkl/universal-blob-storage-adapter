package michaelcirkl.ubsa.client.exception.gcp;

import com.google.cloud.storage.StorageException;
import michaelcirkl.ubsa.client.exception.UbsaException;

import java.io.IOException;

public class GCPSyncExceptionHandler {
    public <T> T handle(IOSupplier<T> action) {
        try {
            return action.get();
        } catch (StorageException error) {
            throw new UbsaException(error.getMessage(), error, error.getCode());
        } catch (IOException error) {
            throw new UbsaException(error.getMessage(), error);
        }
    }

    public interface IOSupplier<T> { // basically just Supplier<T> (function with no args), but can throw IOException.
        T get() throws IOException;
    }
}
