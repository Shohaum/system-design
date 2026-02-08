package snowflake;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
 * Snowflake ID Generator
 *
 * Generates globally unique 64-bit IDs composed of:
 *
 *  | timestamp | workerId | sequence |
 *
 *  timestamp  -> ensures time ordering
 *  workerId   -> identifies machine/node
 *  sequence   -> handles multiple IDs generated within same millisecond
 */

public class Snowflake {
    // Unique ID of this worker/machine
    private final long workerId;

    // Custom epoch (start time) to allow for longer time-based ID usage
    private final long epoch = 1672502400000L;

    // Number of bits allocated for workerId
    private final long workerBits = 10L;
    // Maximum workerId allowed (2^10 - 1 = 1023)   
    private final long maxWorkerId = ~(-1L << workerBits);

    // Number of bits allocated for sequence within same millisecond
    private final long sequenceBits = 12L;
    // Maximum sequence number (2^12 - 1 = 4095)
    private final long maxSequence = ~(-1L << sequenceBits);

    // Current sequence number
    private long sequence = 0L;
    // Last timestamp when ID was generated
    private long lastTimestamp = -1L;

    // Bit shifts to position workerId and timestamp correctly
    private final long workerShift = sequenceBits;
    private final long timestampShift = sequenceBits + workerBits;

    /*
     * Constructor validates workerId range
     */
    public Snowflake(long workerId) {
        if (workerId < 0 || workerId > maxWorkerId) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        this.workerId = workerId;
    }

    /*
     * Generates next unique ID
     * Thread-safe using synchronized keyword
     */
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();

        // If system clock moves backward -> fail (to avoid duplicate IDs)
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }
        // If same millisecond -> increment sequence
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & maxSequence;
            // If sequence overflows -> wait for next millisecond
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // Reset sequence for new millisecond
            sequence = 0L;
        }

        lastTimestamp = timestamp;
        /*
         * Compose final 64-bit ID using bit shifting
         */
        return ((timestamp - epoch) << timestampShift) |
                (workerId << workerShift) |
                sequence;
    }

    /*
     * Helper method to wait for next millisecond
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    /*
     * Prints a few IDs to check format and ordering
     */
    private void testFunctionalCorrectness(Snowflake sf) {
        for (int i = 0; i < 10; i++) {
            System.out.println(sf.nextId());
        }
    }

    /*
     * Tests behavior when sequence reaches max capacity
     */
    private void testSequenceOverflow(Snowflake sf){
        for (int i = 0; i < maxSequence; i++) {
            System.out.println(sf.nextId());
        }
    }

    /*
     * Concurrency test:
     * Multiple threads generate IDs simultaneously
     * and we verify uniqueness using ConcurrentHashMap set.
     */
    private void testConcurrncy(Snowflake sf) throws InterruptedException{
        ExecutorService pool = Executors.newFixedThreadPool(50);

        Set<Long> ids = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < 100000; i++) {
            pool.submit(() -> {
                ids.add(sf.nextId());
            });
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);

        // If implementation correct -> size should equal number of requests
        System.out.println("Generated: " + ids.size());
    }

    /*
     * Tests behavior when system clock moves backward
     */
    private void testClockMoveBackwards(Snowflake sf){
        sf.lastTimestamp = System.currentTimeMillis() + 1000;
        sf.nextId();
    }

    /*
     * Measures how many IDs can be generated per millisecond
     */
    private void testThroughput(Snowflake sf){
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            sf.nextId();
        }
        long end = System.currentTimeMillis();
        System.out.println("Throughput: " + (1_000_000 / (end - start)) + " ids/ms");
    }
    public static void main(String[] args) {
        // Snowflake sf = new Snowflake(1);

        // functional correctness test
        // sf.testFunctionalCorrectness(sf);

        // sequence overflow test
        // sf.testSequenceOverflow(sf);

        // concurrency test
        // try {
        //     sf.testConcurrncy(sf);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }

        // clock move backwards test
        // sf.testClockMoveBackwards(sf); // expected: RuntimeException: Clock moved backwards

        // throughput test
        // sf.testThroughput(sf); // 3676 ids/ms
    }
}
