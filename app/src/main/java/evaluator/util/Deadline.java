package evaluator.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

/**
 * Cooperative deadline utilities for evaluation-time cancellation.
 */
public final class Deadline {
    private Deadline() {
    }

    public static long afterMillis(int timeoutMs) {
        if (timeoutMs <= 0) {
            return Long.MAX_VALUE;
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long now = System.nanoTime();
        long deadline = now + timeoutNanos;
        if (deadline < 0L) {
            return Long.MAX_VALUE;
        }
        return deadline;
    }

    public static void check(long deadlineNanos) {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Interrupted");
        }
        if (deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos) {
            throw new Exceeded();
        }
    }

    public static final class Exceeded extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public Exceeded() {
            super("Deadline exceeded");
        }
    }
}
