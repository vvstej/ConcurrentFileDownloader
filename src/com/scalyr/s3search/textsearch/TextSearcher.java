package com.scalyr.s3search.textsearch;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;

/**
 * A TextSearcher is constructed from a search string, and is able to quickly find instances of that
 * string in UTF-8 encoded text.  To make things interesting, we search for the given string or
 * any of N strings one transpose or replace away from it.
 */
public class TextSearcher {
  /**
   * A BoyerMooreSearchImpl instance for our searchString.
   */
  private final BoyerMooreSearchImpl[] searchImpls;

  /**
   * Construct a TextSearcher to look for instances of the given string, or close permutations thereof.
   */
  public TextSearcher(String searchString) {
    this(searchString, true);
  }

  /**
   * Construct a TextSearcher to look for instances of the given string and, optionally, close permutations thereof.
   */
  public TextSearcher(String searchString, boolean includeEdits) {
    String[] edits = includeEdits ? getEdits(searchString) : new String[] { searchString };

    System.out.format("Searching for %d variations of \"%s\"\n", edits.length, searchString);

    searchImpls = Stream.of(edits)
      .map(TextSearcher::getUTF8Bytes)
      .map(bytes -> new BoyerMooreSearchImpl(bytes))
      .toArray(size -> new BoyerMooreSearchImpl[size]);
  }


  /**
   * Return the number of (case-sensitive) matches for our search string and its permutations
   * in a block of UTF-8 encoded text.
   *
   * @param blob Buffer in which the text to be searched is stored.
   * @param startOffset Byte offset (inclusive) where we begin searching.
   * @param endOffset Byte offset (exclusive) where we stop searching.
   * @return The number of matches in blob[startOffset ... endOffset).
   */
  public int countMatchesInBlob(byte[] blob, final int startOffset, final int endOffset) {
    int matchCount = 0;

    // there are faster ways of searching for N strings at once ... but, in this simulation,
    // the optimized search time is too fast relative to the network time, and we want them
    // to be more balanced.  So we do it this simple way
    for (BoyerMooreSearchImpl searchImpl : searchImpls) {
      int searchPos = startOffset;
      while (searchPos < endOffset) {
        int nextMatch = searchImpl.hashedSearch(blob, searchPos, endOffset - searchPos);
        if (nextMatch < 0) {
          break;
        }

        matchCount++;
        searchPos += nextMatch + 1;
      }
    }

    return matchCount;
  }


  /** Convenience method to uncheck an exception that will never happen. */
  private static byte[] getUTF8Bytes(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Return a list of same-length strings that are one transpose or replace away from the input string.
   * Adapted from https://github.com/spullara/spellcheck, itself a java port of Norvig: http://norvig.com/spell-correct.html
   */
  private static String[] getEdits(String w) {
    String alphabet = "abcdefghijklmnopqrstuvwxyz";
    int len = w.length();

    Stream<String> transposes = range(0, len - 1)
            .mapToObj(i -> w.substring(0, i) + w.substring(i + 1, i + 2) + w.substring(i, i + 1) + w.substring(i + 2));
    Stream<String> replaces = range(0, len).boxed()
            .flatMap(i -> alphabet.chars().mapToObj(c -> w.substring(0, i) + (char) c + w.substring(i + 1)));

    return concat(transposes, replaces).toArray(size -> new String[size]);
  }

  @SafeVarargs
  private static Stream<String> concat(Stream<String>... streams) {
    return Stream.of(streams).flatMap(s -> s);
  }
}


