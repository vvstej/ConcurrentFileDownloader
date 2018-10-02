package com.scalyr.s3search;

import com.google.common.collect.Lists;
import com.scalyr.s3.search.objectstore.client.SimulatedS3Client;
import com.scalyr.s3.search.objectstore.client.SimulatedS3Client.FlakyNetworkException;
import com.scalyr.s3search.textsearch.TextSearcher;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * A naive single-threaded implementation of searching for a string in multiple S3 objects.
 *
 * A partial list of the things it does not do:
 *
 * - Measure execution time - Handle network exceptions - Use threads or tasks
 *
 */
public class Main {

    public static void main(String[] args) throws FlakyNetworkException {
        String searchTerm = args.length > 0 ? args[0] : "phon";
        serialSearch(searchTerm);
        parallelSearch(searchTerm);
    }

    private static void parallelSearch(final String searchTerm) {
        System.out.println("Searching parallely...");
        FileDownloadAndSearchRequest r1 =
                new FileDownloadAndSearchRequest(new SimulatedS3Client(), "s3SimulationFiles", 0, 100);
        List<FileDownloadAndSearchRequest> request = Lists.newArrayList(r1);
        FileSearchDriver driver = new FileSearchDriver(30, 5, request, new TextSearcher(searchTerm), searchTerm);
        driver.downloadAndSearch();
    }

    private static void serialSearch(final String searchTerm) throws FlakyNetworkException {
        System.out.println("Searching serially..");
        Instant start = Instant.now();
        SimulatedS3Client s3Client = new SimulatedS3Client();

        int result = countMatchesInEpochs(0, 100, searchTerm, s3Client);
        Instant end = Instant.now();
        long timeElapsed = Duration.between(start, end).toMillis();
        System.out.println("Search time in milliseconds: " + timeElapsed);
        System.out.format("%d matches found for '%s' and variants%n", result, searchTerm);

    }

    /**
     * Return the number of matches for searchString in each of a series of "epochs" stored in Amazon S3.
     *
     * @param startEpoch Index of the first epoch to search (inclusive).
     * @param endEpoch Index just after the last epoch to search (exclusive).
     * @param searchString String to search for.
     * @param s3Client Accessor used to read the files to search.
     * @return The count of matches across all epochs.
     */
    public static int countMatchesInEpochs(int startEpoch, int endEpoch, String searchString,
            SimulatedS3Client s3Client) throws FlakyNetworkException {
        TextSearcher searcher = new TextSearcher(searchString);

        int[] results = new int[endEpoch - startEpoch];
        for (int epochIndex = startEpoch; epochIndex < endEpoch; epochIndex++) {
            byte[] epochData = s3Client.readFileFromS3("s3SimulationFiles", "epoch_" + epochIndex);
            results[epochIndex - startEpoch] = searcher.countMatchesInBlob(epochData, 0, epochData.length);
        }

        return Arrays.stream(results).sum();
    }

}
