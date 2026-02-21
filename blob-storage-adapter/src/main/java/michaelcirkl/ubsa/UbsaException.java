package michaelcirkl.ubsa;

public class UbsaException extends RuntimeException {
    public UbsaException(String message, RuntimeException nativeException) {
        super(message, nativeException);
    }
}
