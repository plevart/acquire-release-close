package arc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


public class TestARC implements AutoCloseable {

    static void main() {
        int cpus = Runtime.getRuntime().availableProcessors();
        long loops = 5000000L;
        try (var arc = new AcquireReleaseClose.Striped()) {
            testRuns(cpus, loops, arc, 3, 3);
        }
    }

    static void testRuns(int cpus, long loops, AcquireReleaseClose arc, int warmups, int measurements) {
        for (int i = 1; i <= warmups; i++) {
            System.out.println("** " + arc.getClass().getSimpleName() + ", warmup run #" + i + "...");
            testRun(cpus, loops, arc);
        }
        for (int i = 1; i <= measurements; i++) {
            System.out.println("** " + arc.getClass().getSimpleName() + ", measure run #" + i + "...");
            testRun(cpus, loops, arc);
        }
        System.out.println("** end.");
    }

    static void testRun(int cpus, long loops, AcquireReleaseClose arc) {
        try (var test = new TestARC()) {
            // warmup
            Long referenceNs = null;
            var concurrencies = concurrencies(cpus);
            for (int i = 0; i < concurrencies.length; i++) {
                long ns = test.test(concurrencies[i], loops, arc, referenceNs);
                if (i == 0) {
                    referenceNs = ns;
                }
            }
        }
    }

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private volatile boolean go;
    private final AtomicInteger ready = new AtomicInteger(0);

    @Override
    public void close() {
        executor.shutdown();
    }

    long test(int concurrency, long loops, AcquireReleaseClose arc, Long referenceNs) {
        go = false;
        ready.set(0);
        var cfs = IntStream
            .range(0, concurrency)
            .mapToObj(_ -> CompletableFuture.supplyAsync(
                () -> {
                    ready.incrementAndGet();
                    while (!go) {
                        Thread.onSpinWait();
                    }
                    long counter = 0;
                    for (long l = 0; l < loops; l++) {
                        arc.acquire();
                        try {
                            counter++;
                        } finally {
                            arc.release();
                        }
                    }
                    return counter;
                },
                executor
            ))
            .toArray(CompletableFuture<?>[]::new);

        while (ready.get() < concurrency) {
            Thread.onSpinWait();
        }
        long t0 = System.nanoTime();
        go = true;
        CompletableFuture.allOf(cfs).join();
        long ns = System.nanoTime() - t0;

        System.out.printf(
            "concurrency: %3d, time: %12d ns, latency: %9.4f ns/op - (x %.4f)\n",
            concurrency,
            ns,
            (double) ns / (double) loops,
            referenceNs != null ? (double) ns / (double) referenceNs : 1.0
        );

        return ns;
    }

    static int[] concurrencies(int cpus) {
        var factors = factorize(cpus);
        var concurrencies = IntStream.builder();
        int product = 1;
        concurrencies.add(product);
        for (var factor : factors) {
            int p = product;
            for (int f = 2; f <= factor; f++) {
                p = product * f;
                concurrencies.add(p);
            }
            product = p;
        }
        return concurrencies.build().toArray();
    }


    static int[] factorize(int n) {
        var factors = IntStream.builder();
        while (n % 2 == 0) {
            factors.add(2);
            n /= 2;
        }
        for (int i = 3; i <= Math.sqrt(n); i += 2) {
            while (n % i == 0) {
                factors.add(i);
                n /= i;
            }
        }
        if (n > 2) {
            factors.add(n);
        }
        return factors.build().toArray();
    }
}