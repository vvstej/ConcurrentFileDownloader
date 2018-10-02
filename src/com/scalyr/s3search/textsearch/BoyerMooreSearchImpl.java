package com.scalyr.s3search.textsearch;

import com.scalyr.s3search.utilities.ByteUtils;

/**
 * Implements an optimized Boyer-Moore search algorithm.
 */
public class BoyerMooreSearchImpl {
  protected static final int HASH_SIZE = 65536;

  /**
   * The pattern we search for.
   */
  protected final byte[] pattern;

  protected final int patternLength;

  /**
   * Maps bigrams (two-byte subsequences of the pattern) to each position in the pattern where that bigram appears,
   * plus one.
   */
  protected final byte[] skipHash;

  /**
   * A copy of the pattern. If caseSensitive is false, then all letters are converted to lowercase.
   */
  protected final byte[] normalizedPattern;

  /**
   * An array of patternLength. If caseSensitive is false, holds 32 for each letter in the pattern,
   * 0 elsewhere. Otherwise all zeros. Can be ORed into a value to allow case-insensitive comparison
   * with normalizedPattern.
   */
  protected final byte[] letterMask;

  /**
   * Holds a lowercased copy of the first four bytes of the pattern, i.e. the bytes at 1 through
   * 4 inclusive, in native endian order. Undefined if the pattern is less than four bytes long.
   */
  protected final int fourAtStart;

  /**
   * Holds a value which can be ORed into a four-character sequence to eliminate case variations before
   * comparing with fourAtStart. Each byte is 32 if the corresponding character in fourAtStart is a
   * lowercase letter, 0 otherwise.
   */
  protected final int fourAtStartLowercaseMask;

  /**
   * For each byte value, holds the rightmost index in the pattern where that byte value appears. If the
   * byte value does not appear anywhere in the pattern, holds -1. For non-case-sensitive search, each
   * entry holds the rightmost index where a non-case-sensitive match for that byte value appears.
   */
  protected final int[] skips;

  /**
   * Construct a searcher that will look for instances of a specified byte pattern.
   *
   * @param pattern The pattern to search for.
   */
  protected BoyerMooreSearchImpl(byte[] pattern) {
    this.pattern = pattern;
    this.patternLength = pattern.length;

    if (patternLength >= 4) {
      byte[] temp = new byte[4], temp2 = new byte[4];
      System.arraycopy(pattern, 0, temp, 0, 4);

      fourAtStartLowercaseMask = ByteUtils.getIntUnsafeLocalEndian(temp2, 0);
      fourAtStart = ByteUtils.getIntUnsafeLocalEndian(temp, 0) | fourAtStartLowercaseMask;
    } else {
      fourAtStart = 0;
      fourAtStartLowercaseMask = 0;
    }

    normalizedPattern = new byte[patternLength];
    letterMask = new byte[patternLength];
    System.arraycopy(pattern, 0, normalizedPattern, 0, patternLength);

    if (patternLength >= 3 && patternLength <= 254) {
      skipHash = new byte[HASH_SIZE];
    } else {
      skipHash = null;
    }

    skips = new int[256];
    for (int i=0; i < 256; i++)
      skips[i] = -1;

    for (int i = 0; i < pattern.length; i++) {
      int patternValue = pattern[i] & 255;
      skips[patternValue] = i;
    }

    if (skipHash != null) {
      for (int patternIndex = patternLength - 2; patternIndex >= 0; patternIndex--) {
        addToSkipHashWithVariants(pattern, patternIndex);
      }
    }
  }

  protected void addToSkipHashWithVariants(byte[] pattern, int patternIndex) {
    int hashIndex = ByteUtils.getShortUnsafeLocalEndian(pattern, patternIndex) & (HASH_SIZE-1);
    addToSkipHash(patternIndex, hashIndex);
  }

  protected void addToSkipHash(int patternIndex, int hashIndex) {
    while (skipHash[hashIndex] != 0)
      hashIndex = (hashIndex + 1) & (HASH_SIZE-1);
    skipHash[hashIndex] = (byte) (patternIndex + 1);
  }

  protected boolean matchAllBut4(byte[] buffer, int candidatePos) {
    for (int i = 4; i < patternLength; i++)
      if ((buffer[candidatePos + i] | letterMask[i]) != normalizedPattern[i])
        return false;

    return true;
  }

  /**
   * Return the first starting position of the pattern in the bufferLength bytes beginning at bufferOffset,
   * or -1 if the pattern does not occur. Our result is relative to bufferOffset, e.g. if we find a match
   * starting at buffer[bufferOffset + 2], we return 2.
   */
  public int simpleSearch(byte[] buffer, int bufferOffset, int bufferLength) {
    // Loop invariant: there are no matches beginning prior to searchPos.
    int searchPos = bufferOffset;
    while (searchPos <= bufferLength + bufferOffset - patternLength) {
      // Compare the pattern against the buffer, working from right to left. If we find a mismatch, advance
      // according to the skips table.
      boolean mismatch = false;
      for (int patternIndex = patternLength - 1; patternIndex >= 0; patternIndex--) {
        byte b = buffer[searchPos + patternIndex];
        if (pattern[patternIndex] != b) {
          int skipValue = skips[b & 255];

          // We found byte b aligned to position patternIndex in the pattern, but it can't appear farther to the right than skipValue.
          // So we can advance by patternIndex - skipValue. Hopefully patternIndex is large and skipValue is small (e.g. -1),
          // in which case we will advance quite a bit, possibly as much as patternLength.
          if (skipValue >= patternIndex) {
            searchPos++;
            mismatch = true;
            break;
          } else {
            searchPos += patternIndex - skipValue;
            mismatch = true;
            break;
          }
        }
      }
      if (!mismatch) {
        return searchPos - bufferOffset;
      }
    }

    return -1;
  }

  /**
   * Return the first starting position of the pattern in the bufferLength bytes beginning at bufferOffset,
   * or -1 if the pattern does not occur. Our result is relative to bufferOffset, e.g. if we find a match
   * starting at buffer[bufferOffset + 2], we return 2.
   *
   * This implementation uses a hash table to try all possible pattern alignments based on a 16-bit sample
   * of the haystack.
   */
  public int hashedSearch(byte[] buffer, int bufferOffset, int bufferLength) {
    if (pattern.length == 0)
      return 0;

    // Don't use this version unless the buffer has at least patternLength extra bytes. This
    // protects us against bad parameter values, as well as the remote possibility of a segment
    // fault: we sometimes read (speculatively) a bit past the end of the buffer.
    if (patternLength < 4 || bufferOffset + bufferLength + patternLength > buffer.length || skipHash == null)
      return simpleSearch(buffer, bufferOffset, bufferLength);

    for (int searchPos = bufferOffset + patternLength - 2; searchPos <= bufferOffset + bufferLength - 2;
        searchPos += patternLength - 1) {
      int hashIndex = ByteUtils.getShortUnsafeLocalEndian(buffer, searchPos) & (HASH_SIZE-1);
      while (true) {
        int hashValue = skipHash[hashIndex] & 255;
        if (hashValue == 0)
          break;

        int candidatePos = searchPos - (hashValue - 1);
        if (candidatePos <= bufferOffset + bufferLength - patternLength &&
            (ByteUtils.getIntUnsafeLocalEndian(buffer, candidatePos) | fourAtStartLowercaseMask) == fourAtStart &&
            matchAllBut4(buffer, candidatePos))
          return candidatePos - bufferOffset;

        hashIndex = (hashIndex + 1) & (HASH_SIZE-1);
      }
    }

    return -1;
  }
}
