package michaelcirkl.ubsa.client.streaming;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileUploadValidatorsTest {
    @TempDir
    Path tempDir;

    @Test
    void validateSourceFileRejectsNullMissingAndDirectories() throws Exception {
        IllegalArgumentException nullError = assertThrows(
                IllegalArgumentException.class,
                () -> FileUploadValidators.validateSourceFile(null)
        );
        assertTrue(nullError.getMessage().contains("must not be null"));

        Path missing = tempDir.resolve("missing.txt");
        IllegalArgumentException missingError = assertThrows(
                IllegalArgumentException.class,
                () -> FileUploadValidators.validateSourceFile(missing)
        );
        assertTrue(missingError.getMessage().contains("does not exist"));

        Path directory = Files.createDirectory(tempDir.resolve("folder"));
        IllegalArgumentException directoryError = assertThrows(
                IllegalArgumentException.class,
                () -> FileUploadValidators.validateSourceFile(directory)
        );
        assertTrue(directoryError.getMessage().contains("regular file"));
    }

    @Test
    void validateSourceFileAcceptsReadableRegularFile() throws Exception {
        Path file = Files.writeString(tempDir.resolve("upload.txt"), "validator");
        assertDoesNotThrow(() -> FileUploadValidators.validateSourceFile(file));
    }
}
