package arc;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.LongAdder;

public sealed interface AcquireReleaseClose extends AutoCloseable {

    void acquire();

    void release();

    final class Simple implements AcquireReleaseClose {
        private static final VarHandle ACQUIRE_COUNT;

        static {
            try {
                ACQUIRE_COUNT = MethodHandles.lookup().findVarHandle(Simple.class, "acquireCount", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }

        private volatile int acquireCount;

        @Override
        public void acquire() {
            int value;
            do {
                value = acquireCount;
                if (value < 0) {
                    //segment is not open!
                    throw new IllegalStateException("Already closed");
                } else if (value == Integer.MAX_VALUE) {
                    //overflow
                    throw new IllegalStateException("Too many acquires");
                }
            } while (!ACQUIRE_COUNT.compareAndSet(this, value, value + 1));
        }

        @Override
        public void release() {
            int value;
            do {
                value = acquireCount;
                if (value <= 0) {
                    //cannot get here - we can't close segment twice
                    throw new IllegalStateException("Already closed");
                }
            } while (!ACQUIRE_COUNT.compareAndSet(this, value, value - 1));
        }

        @Override
        public void close() {
            int value = (int) ACQUIRE_COUNT.compareAndExchange(this, 0, -1);
            if (value < 0) {
                throw new IllegalStateException("Already closed");
            } else if (value > 0) {
                throw new IllegalStateException("Still acquired by " + value + " threads");
            }
        }
    }

    final class Striped implements AcquireReleaseClose {
        private static final int OPEN = 0, MAYBE_CLOSING = 1, CLOSED = 2;
        private static final VarHandle CLOSING_PHASE;

        static {
            try {
                CLOSING_PHASE = MethodHandles.lookup().findVarHandle(Striped.class, "closingPhase", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new AssertionError(e);
            }
        }

        private volatile int closingPhase;
        private final LongAdder acquireCount = new LongAdder();
        private final LongAdder releaseCount = new LongAdder();

        @Override
        public void acquire() {
            acquireCount.increment();
            // above write to acquireCount Cell may have been reordered after below read of closingPhase if
            // it was not for this fullFence that prevents this from happening
            VarHandle.fullFence();
            int cp;
            while ((cp = closingPhase) != OPEN) {
                if (cp == CLOSED) {
                    releaseCount.increment(); // not required, but useful for debugging
                    throw new IllegalStateException("Already closed");
                }
                Thread.onSpinWait();
            }
        }

        @Override
        public void release() {
            releaseCount.increment();
        }

        @Override
        public void close() {
            int cp;
            while ((cp = (int) CLOSING_PHASE.compareAndExchange(this, OPEN, MAYBE_CLOSING)) != OPEN) {
                if (cp == CLOSED) {
                    throw new IllegalStateException("Already closed");
                }
                Thread.onSpinWait();
            }
            // above write to closingPhase may have been reordered after below reads of releaseCount and acquireCount cells if
            // it was not for this fullFence that prevents this from happening
            VarHandle.fullFence();
            // reading order is important: 1st RELEASE_COUNT, 2nd ACQUIRE_COUNT
            long value = -releaseCount.longValue() + acquireCount.longValue();
            if (value > 0) {
                closingPhase = OPEN;
                throw new IllegalStateException("Still acquired by " + value + " threads");
            }
            closingPhase = CLOSED;
        }
    }
}
