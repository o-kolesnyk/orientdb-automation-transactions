package utils;

import java.util.concurrent.atomic.AtomicLong;

public class Counter {

    private static AtomicLong vertexCounter = new AtomicLong(0);

    public static long getNextVertexId() {
        return vertexCounter.incrementAndGet();
    }

    public static long getVertexesNumber() {
        return vertexCounter.get();
    }
}
