package michaelcirkl.ubsa.client.streaming;

public final class ByteArrayRangeValidator {
    private ByteArrayRangeValidator() {
    }

    public static long validateAndGetLength(long startInclusive, long endInclusive) {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range. startInclusive must be >= 0 and endInclusive must be >= startInclusive.");
        }

        long rangeWidth = endInclusive - startInclusive;
        if (rangeWidth >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Requested byte range exceeds the maximum supported in-memory byte[] length of " + Integer.MAX_VALUE + " bytes."
            );
        }

        return rangeWidth + 1L;
    }
}
