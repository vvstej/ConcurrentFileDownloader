package com.scalyr.s3search.utilities;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A fast, medium-quality random number generator, inspired by
 * http://www.javamex.com/tutorials/random_numbers/xorshift.shtml#.UcjtnfY6WWU. Threadsafe.
 */
public class FastRandom {
  private volatile long x;

  private static final AtomicLongFieldUpdater<FastRandom> updater =
      AtomicLongFieldUpdater.newUpdater(FastRandom.class, "x");

  public FastRandom() {
    this(System.nanoTime());
  }

  public FastRandom(long seed) {
    this.x = seed;
  }

  public int nextInt() {
    return (int) nextLong();
  }

  public int nextPositiveInt() {
    return nextInt() & 0x7FFFFFFF;
  }

  public int nextInt(int maxValue) {
    if ((maxValue & -maxValue) == maxValue) { // i.e., maxValue is a power of 2
      return (int) ((maxValue * (long) nextPositiveInt()) >> 31);
    }
    while (true) {
      int bits = nextPositiveInt();
      int val = bits % maxValue;
      if (bits - val + (maxValue - 1) >= 0) {
        return val;
      }
    }
  }

  public int nextBits(int bitCount) {
    return (int) (nextLong() >>> (64 - bitCount));
  }

  /**
   * Return a number uniformly chosen from the range [0, 1).
   */
  public double nextDouble() {
    return (((long) nextBits(26) << 27) + nextBits(27)) / (double) (1L << 53);
  }

  public long nextLong() {
    long nextValue;

    while (true) {
      long currentValue = x;

      nextValue = x;
      nextValue ^= (nextValue << 21);
      nextValue ^= (nextValue >>> 35);
      nextValue ^= (nextValue << 4);

      if (updater.compareAndSet(this, currentValue, nextValue)) {
        break;
      }
    }

    return nextValue;
  }
}
