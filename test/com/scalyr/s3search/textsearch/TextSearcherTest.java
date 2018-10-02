package com.scalyr.s3search.textsearch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for TextSearcher.
 */
public class TextSearcherTest {
  @Test public void test() {
    TextSearcher searcher = new TextSearcher("foo", false);

    assertEquals(0, countMatchesInBlob(searcher, "", 0, 0));
    assertEquals(2, countMatchesInBlob(searcher, "foo bar baz foo", 0, 0));
    assertEquals(2, countMatchesInBlob(searcher, "foo bar baz foo", 123, 234));
    assertEquals(0, countMatchesInBlob(searcher, "abcdefghijfoxyz", 10, 20));
    assertEquals(0, countMatchesInBlob(searcher, "ofo", 10, 20));
    assertEquals(3, countMatchesInBlob(searcher, "abcfoofoofoodef", 3, 7));
  }

  private int countMatchesInBlob(TextSearcher searcher, String textToSearch, int paddingBefore,
                                    int paddingAfter) {
    byte[] buffer = textToSearch.getBytes();
    byte[] paddedBuffer = new byte[buffer.length + paddingBefore + paddingAfter];

    for (int i = 0; i < paddedBuffer.length; i++)
      paddedBuffer[i] = buffer[(i - paddingBefore + buffer.length * 1000) % buffer.length];

    return searcher.countMatchesInBlob(paddedBuffer, paddingBefore, paddingBefore + buffer.length);
  }
}
