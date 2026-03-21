package michaelcirkl.ubsa.client.exception;

public class UbsaException extends RuntimeException {
    private int statusCode;
    public UbsaException(String message, Throwable nativeException) {
        super(message, nativeException);
    }

    public UbsaException(String message, Throwable nativeException, int statusCode) {
        super(message, nativeException);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
