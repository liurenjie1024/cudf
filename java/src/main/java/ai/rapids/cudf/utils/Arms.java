package ai.rapids.cudf.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Arms {
    public static <R extends AutoCloseable, T> T closeIfException(R resource, Function<R, T> function) {
        try {
            return function.apply(resource);
        } catch (Exception e) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception inner) {
                    // ignore
                }
            }
            throw e;
        }
    }

    public static <R extends AutoCloseable> void closeQuietly(Iterator<R> resources) {
        while (resources.hasNext()) {
            try {
                resources.next().close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static <R extends AutoCloseable> void closeQuietly(R... resources) {
        closeQuietly(Arrays.stream(resources).collect(Collectors.toList()));
    }

    public static <R extends AutoCloseable> void closeQuietly(Iterable<R> resources) {
        closeQuietly(resources.iterator());
    }
}
