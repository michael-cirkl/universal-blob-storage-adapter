package michaelcirkl.ubsa;

public class UbsaException extends RuntimeException {
    public UbsaException(String message, Throwable nativeException) {
        super(message, nativeException);
    }

    public UbsaException(String message) {
        super(message);
    }
}
