package io.github.michaelcirkl.ubsa.client.streaming;

import java.nio.file.Files;
import java.nio.file.Path;

public final class FileUploadValidators {
    private FileUploadValidators() {
    }

    public static void validateSourceFile(Path sourceFile) {
        if (sourceFile == null) {
            throw new IllegalArgumentException("Source file path must not be null.");
        }
        if (!Files.exists(sourceFile)) {
            throw new IllegalArgumentException("Source file does not exist: " + sourceFile);
        }
        if (!Files.isRegularFile(sourceFile)) {
            throw new IllegalArgumentException("Source file must be a readable regular file: " + sourceFile);
        }
        if (!Files.isReadable(sourceFile)) {
            throw new IllegalArgumentException("Source file must be readable: " + sourceFile);
        }
    }
}
