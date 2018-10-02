package com.scalyr.s3search;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Hacky code for creating files full of English words.
 */
public class FileCreator {
  public static void main(String[] args) {
    File directory = new File("s3SimulationFiles");
    System.out.println("Writing 100 x 500KB files to " + directory + "/");

    int epochCount = 100;
    int epochFileLength = 500_000;

    Random rng = new Random(123);
    List<String> words = readWordList();
    shuffle(words, rng);

    directory.mkdir();
    for (int epochIndex = 0; epochIndex < epochCount; epochIndex++) {
      File epochFile = new File(directory, "epoch_" + epochIndex);
      String epochText = buildRandomText(words, epochFileLength, rng);
      writeStringToFile(epochText, epochFile);
    }
  }

  /**
   * Return a string of approximately the specified length, built by concatenating random entries
   * from the given word list.
   */
  private static String buildRandomText(List<String> words, int length, Random rng) {
    StringBuilder sb = new StringBuilder();

    int maxLineLength = 80;
    int lengthBeforeCurrentLine = 0;
    while (true) {
      // Use a simple power law to choose words from the dictionary.
      double x = Math.pow(rng.nextDouble(), 4);
      int wordIndex = (int) Math.floor(x * words.size());
      String word = words.get(wordIndex);

      if (sb.length() + word.length() + 1 > length)
        break;

      if (sb.length() > 0) {
        int charsInLine = sb.length() - lengthBeforeCurrentLine;
        if (charsInLine + 1 + word.length() > maxLineLength) {
          sb.append("\n");
          lengthBeforeCurrentLine = sb.length();
        } else {
          sb.append(" ");
        }
      }

      sb.append(word);
    }

    return sb.toString();
  }

  /**
   * Read the file containing the word list, and split it up into words.
   */
  private static List<String> readWordList() {
    File wordlistFile = new File("wordlist.txt");
    String wordlist = readFileAsString(wordlistFile);
    List<String> words = new ArrayList<>();
    for (String s : wordlist.split("[\\r\\n]")) {
      String trimmed = s.trim();
      if (trimmed.length() > 0) {
        words.add(trimmed);
      }
    }

    return words;
  }

  private static void shuffle(List<String> words, Random rng) {
    for (int i = 0; i < words.size() - 1; i++) {
      int j = i + 1 + rng.nextInt(words.size() - i - 1);
      String temp = words.get(i);
      words.set(i, words.get(j));
      words.set(j, temp);
    }
  }

  /**
   * Read a file, and return its contents as a string.
   */
  private static String readFileAsString(File file) {
    try {
      FileInputStream stream = new FileInputStream(file);
      try {
        Reader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        char[] buffer = new char[4096];
        while (true) {
          int count = reader.read(buffer, 0, buffer.length);
          if (count <= 0)
            break;

          sb.append(buffer, 0, count);
        }
        return sb.toString();
      } finally {
        stream.close();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void writeStringToFile(String text, File file) {
    try {
      if (file.exists())
        file.delete();

      FileOutputStream output = new FileOutputStream(file, false);
      OutputStreamWriter writer = new OutputStreamWriter(output, Charset.forName("UTF-8"));
      writer.write(text);
      writer.flush();
      writer.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
