package com.scalyr.s3search.s3simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

/**
 * Simulates delays for transmitting data over a network.
 */
public class NetworkSimulator {
  /**
   * Simulated bandwidth available to the local node, in megabits per second.
   */
  private final double bandwidthMbps;

  /**
   * Maximum bandwidth for a single network stream, in megabits per second.
   */
  private final double maxBandwidthPerStreamMbps;

  /**
   * All outstanding network operations. Synchronize access on the NetworkSimulator.
   */
  private final List<Operation> operations = new ArrayList<Operation>();

  /**
   * System.currentTimeMillis() value when we last updated the simulation state. Synchronize access on the NetworkSimulator.
   */
  private long lastUpdateTime;

  private final Timer timer = new Timer("NetworkSimulator", true);

  /** Construct a NetworkSimulator using default bandwidth values. */
  public NetworkSimulator() {
    this(1000, 250);
  }

  /**
   * Construct a NetworkSimulator.
   *
   * @param bandwidthMbps Simulated bandwidth available to the local node, in megabits per second.
   * @param maxBandwidthMbps Maximum per-stream bandwidth (less than bandwidthMbps), in megabits per second.
   */
  public NetworkSimulator(double bandwidthMbps, double maxBandwidthPerStreamMbps) {
    this.bandwidthMbps = bandwidthMbps;
    this.maxBandwidthPerStreamMbps = maxBandwidthPerStreamMbps;
  }

  public void shutdown() {
    timer.cancel();
  }

  /**
   * Pause the current thread for the simulated time needed to read the given number of bytes from
   * the network.
   */
  public void waitForTraffic(long bytes) {
    Operation operation = new Operation(bytes);

    synchronized (this) {
      long currentTime = System.currentTimeMillis();
      updateTime(currentTime);

      operations.add(operation);

      addTimerTaskForNextOperationCompletion();
    }

    try {
      operation.completionSemaphore.acquire();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Advance the simulation to the specified time.
   */
  private synchronized void updateTime(long currentTime) {
    if (operations.size() == 0) {
      lastUpdateTime = currentTime;
      return;
    }

    while (lastUpdateTime < currentTime) {
      double bytesPerOperationPerSecond = networkRatePerStream(operations.size());

      // Determine whether any operation will complete before timeDelta.
      double smallestBytesRemaining = getSmallestBytesRemaining();
      long timeDelta = currentTime - lastUpdateTime;
      double bytesForDelta = bytesPerOperationPerSecond * timeDelta / 1000.0;
      // System.out.println("advanceOperations: timeDelta = " + timeDelta + ", bytesForDelta = " + (int)bytesForDelta + ", smallestBytesRemaining = " + (int)smallestBytesRemaining);
      if (bytesForDelta >= smallestBytesRemaining) {
        advanceOperations(smallestBytesRemaining);
        lastUpdateTime += Math.ceil(smallestBytesRemaining / bytesPerOperationPerSecond * 1000.0);
      } else {
        advanceOperations(bytesForDelta);
        lastUpdateTime = currentTime;
        break;
      }
    }

    addTimerTaskForNextOperationCompletion();
  }

  /**
   * If there are any outstanding operations, add a timer task to trigger when the next operation is due to complete.
   */
  private synchronized void addTimerTaskForNextOperationCompletion() {
    if (operations.size() > 0) {
      long msToNextCompletion = (long) Math.ceil(getSmallestBytesRemaining() / networkRatePerStream(operations.size()) * 1000.0);
      timer.schedule(new TimerTask() {
        @Override public void run() {
          updateTime(System.currentTimeMillis());
        }
      }, msToNextCompletion);
    }
  }

  /**
   * Advance all outstanding operations by the given number of bytes.
   */
  private void advanceOperations(double byteCount) {
    for (int operationIndex = operations.size() - 1; operationIndex >= 0; operationIndex--) {
      Operation operation = operations.get(operationIndex);
      operation.bytesRemaining -= byteCount;

      // If the operation has fewer than one remaining byte to read, mark it as done. We compare to 1, not 0, so as to avoid
      // rounding error issues.
      if (operation.bytesRemaining <= 1) {
        operation.completionSemaphore.release();
        operations.remove(operationIndex);
      }
    }
  }

  /**
   * Determine the data rate each network operation will achieve, given a specified number of simultaneous operations.
   *
   * @param streamCount The number of in-flight operations streaming data over the network.
   * @return Data rate for each operation, in bytes per second.
   */
  private double networkRatePerStream(int streamCount) {
    double desiredBandwidth = maxBandwidthPerStreamMbps * streamCount;
    double availableBandwidth = bandwidthMbps;
    double subscriptionFactor = desiredBandwidth / availableBandwidth;

    // Reduce the available bandwidth by a congestion factor. As subscriptionFactor approaches and exceeds 1.0, the network
    // becomes oversubscribed, and we assume that bandwidth drops.
    double congestionFactor = Math.max(0, subscriptionFactor * subscriptionFactor / 10.0 - 0.04);

    availableBandwidth *= Math.max(0.3, 1.0 - congestionFactor);

    return Math.min(maxBandwidthPerStreamMbps, availableBandwidth / Math.max(streamCount, 1)) * 1000 * 1000 / 8.0;

  }

  /**
   * Return the smallest bytesRemaining value for any outstanding operation.
   *
   * The caller must hold our lock.
   */
  private double getSmallestBytesRemaining() {
    double result = Double.MAX_VALUE;

    for (Operation operation : operations)
      result = Math.min(result, operation.bytesRemaining);

    return result;
  }

  /**
   * Represents an outstanding network operation which is consuming bandwidth.
   */
  private class Operation {
    /**
     * The number of bytes remaining to be transferred over the network, as of lastUpdateTime.
     */
    double bytesRemaining;

    /**
     * Semaphore which we release once the transfer is complete.
     */
    final Semaphore completionSemaphore = new Semaphore(0);

    Operation(long bytes) {
      this.bytesRemaining = bytes;
    }
  }
}
