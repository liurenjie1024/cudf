package ai.rapids.cudf.utils;

import java.util.function.Supplier;

public class PreConditions {
    public static void ensure(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void ensure(boolean condition, Supplier<String> messageSupplier) {
        if (!condition) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }
}
