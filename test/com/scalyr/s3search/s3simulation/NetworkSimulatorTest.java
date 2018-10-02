package com.scalyr.s3search.s3simulation;

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Tests for NetworkSimulator.
 */
public class NetworkSimulatorTest {
  private NetworkSimulator networkSimulator = null;

  @After public void cleanupNetworkSimulatorTest() {
    networkSimulator.shutdown();
  }

  /**
   * Simple test of single-threaded operations.
   */
  @Test public void testSingleThreadedTiming() {
    networkSimulator = new NetworkSimulator(8, 5);

    // A single thread should run at 5mbps, so transferring 20,000 bytes should take roughly
    // 0.16/5 seconds, or 32ms.
    for (int i = 0; i < 20; i++) {
      System.out.println(measureTime(20000));
    }
  }

  /**
   * Simple test of dual-threaded operations.
   */
  @Test public void testDualThreadedTiming() throws InterruptedException {
    // With two threads, each thread will get half of the 8Mbps bandwidth, so transferring
    // 100,000 bytes should take roughly .8/4 seconds, or 200ms; minus a congestion factor.
    testTimingWithNThreads(2);
  }

  /**
   * Simple test of operations with three threads.
   */
  @Test public void test3ThreadedTiming() throws InterruptedException {
    testTimingWithNThreads(3);
  }

  private void testTimingWithNThreads(int threadCount) throws InterruptedException {
    networkSimulator = new NetworkSimulator(8, 5);

    final Semaphore completionSemaphore = new Semaphore(0);
    Executor executor = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executor.execute(new Runnable() {
        @Override public void run() {
          for (int j = 0; j < 20; j++) {
            System.out.println(measureTime(100000));
          }
          completionSemaphore.release();
        }
      });
    }

    completionSemaphore.acquire(threadCount);
  }

  /**
   * Return the amount of time (in milliseconds) needed to transfer the given amount of data over the
   * simulated network.
   */
  private int measureTime(int bytes) {
    long startTimeMs = System.currentTimeMillis();
    networkSimulator.waitForTraffic(bytes);
    return (int) (System.currentTimeMillis() - startTimeMs);
  }
}
