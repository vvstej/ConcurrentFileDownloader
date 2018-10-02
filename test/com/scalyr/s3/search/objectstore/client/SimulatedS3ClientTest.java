package com.scalyr.s3.search.objectstore.client;

import com.scalyr.s3.search.objectstore.client.SimulatedS3Client;
import com.scalyr.s3search.FileCreator;
import com.scalyr.s3search.s3simulation.NetworkSimulator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for SimulatedS3Client.
 */
public class SimulatedS3ClientTest {
  private File tempDir;
  private File bucket1;
  private File bucket2;
  private NetworkSimulator networkSimulator = null;

  @Before public void setupSimulatedS3ClientTest() throws IOException {
    // Create a temporary directory...
    Path tempDirPath = Files.createTempDirectory("simulatedS3ClientTest");
    tempDir = tempDirPath.toFile();
    tempDir.mkdirs();

    // ... with two buckets below
    bucket1 = tempDirPath.resolve("bucket1").toFile();
    bucket2 = tempDirPath.resolve("bucket2").toFile();
    bucket1.mkdirs();
    bucket2.mkdirs();
  }

  @After public void cleanupSimulatedS3ClientTest() {
    networkSimulator.shutdown();

    // Clean up the temporary directory.
    for (File dir : new File[] { bucket2, bucket1, tempDir }) {
      if (dir == null)
        continue;
      for (File f : dir.listFiles())
        f.delete();
      dir.delete();
    }
  }

  /**
   * A basic test of read correctness.
   */
  @Test public void test() throws IOException {
    createFileWithText(bucket1, "file1", "aaa");
    createFileWithText(bucket1, "file2", "bbb");
    createFileWithText(bucket2, "file1", "ccc");

    networkSimulator = new NetworkSimulator(10, 5);
    SimulatedS3Client client = new SimulatedS3Client(tempDir, networkSimulator, 0.0);

    testReadFile(client, "bucket1", "file1", "aaa");
    testReadFile(client, "bucket1", "file2", "bbb");
    testReadFile(client, "bucket2", "file1", "ccc");
    testReadFile(client, "bucket1", "file1", "aaa");
  }

  /**
   * Test simulated disk-access delays.
   *
   * Currently this isn't a proper test, it simply logs some timings for manual review. A proper test will
   * be a bit of work, since the expected behavior is pseudo-random.
   */
  @Test public void testReadTiming() throws IOException {
    createFileWithText(bucket1, "file1", "aaa");

    networkSimulator = new NetworkSimulator(1000, 1000);

    SimulatedS3Client client = new SimulatedS3Client(tempDir, networkSimulator, 0.0);

    for (int i = 0; i < 100; i++) {
      long startTimeMs = System.currentTimeMillis();
      testReadFile(client, "bucket1", "file1", "aaa");
      System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTimeMs) + " ms");
    }
  }


  /**
   * Invoke client.readFileFromS3(bucketName, objectName). Verify that the data returned is the UTF-8
   * form of expectedResult.
   */
  private void testReadFile(SimulatedS3Client client, String bucketName, String objectName,
                            String expectedResult) {
    try {
      byte[] result = client.readFileFromS3(bucketName, objectName);
      assertArrayEquals(expectedResult.getBytes("UTF-8"), result);
    } catch (UnsupportedEncodingException|SimulatedS3Client.FlakyNetworkException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void createFileWithText(File directory, String filename, String fileContent) {
    FileCreator.writeStringToFile(fileContent, new File(directory, filename));
  }
}
