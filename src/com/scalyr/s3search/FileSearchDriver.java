package com.scalyr.s3search;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.scalyr.s3.search.executors.FileDownloadExecutor;
import com.scalyr.s3.search.executors.TextSearchExecutor;
import com.scalyr.s3.search.objectstore.client.ObjectStoreFile;
import com.scalyr.s3search.textsearch.SearchResult;
import com.scalyr.s3search.textsearch.TextSearcher;

public class FileSearchDriver {

    private final ExecutorService objectStoreFileDownloadExectuor;
    private final ExecutorService fileSearchExecutor;
    private final List<FileDownloadAndSearchRequest> requestObjects;
    private final TextSearcher textSearcher;
    private final String textToSearch;

    public FileSearchDriver(final int fileDownloaderThreadPoolCount, final int fileSearcherThreadPoolCount,
            List<FileDownloadAndSearchRequest> requestObjects, final TextSearcher textSearcher,
            final String textToSearch) {
        this.objectStoreFileDownloadExectuor = Executors.newFixedThreadPool(fileDownloaderThreadPoolCount);
        this.fileSearchExecutor = Executors.newFixedThreadPool(fileSearcherThreadPoolCount);
        this.requestObjects = requestObjects;
        this.textSearcher = textSearcher;
        this.textToSearch = textToSearch;
    }

    public void downloadAndSearch() {
        try {
            final List<CompletableFuture<ObjectStoreFile>> downloadResults = Lists.newArrayList();
            final List<CompletableFuture<SearchResult>> searchResults = Lists.newArrayList();

            Instant start = Instant.now();

            for (final FileDownloadAndSearchRequest request : requestObjects) {
                for (int i = request.startEpoch; i < request.endEpoch; i++) {
                    final String fileName = "epoch_" + i;
                    downloadResults
                            .add(CompletableFuture
                                    .supplyAsync(
                                            () -> new FileDownloadExecutor(request.getRemoteObjectStoreClient(),
                                                    request.bucketName, fileName).download(),
                                            objectStoreFileDownloadExectuor));
                }
            }
            for (CompletableFuture<ObjectStoreFile> fileDownloadFuture : downloadResults) {
                searchResults.add(fileDownloadFuture.thenComposeAsync(file -> CompletableFuture.supplyAsync(
                        () -> new TextSearchExecutor(textSearcher, textToSearch, file).searchFile(),
                        fileSearchExecutor)));
            }

            CompletableFuture<Void> combinedFuture =
                    CompletableFuture.allOf(searchResults.toArray(new CompletableFuture[searchResults.size()]));

            CompletableFuture<List<SearchResult>> results = combinedFuture.thenApply(v -> {
                return searchResults.stream().map(searchResultFuture -> searchResultFuture.join())
                        .collect(Collectors.toList());
            });
            
            long countOfMatches =
                    results.join().stream().mapToInt(searchResult -> searchResult.getNumberOfMatches()).sum();
            Instant end = Instant.now();
            long timeElapsed = Duration.between(start, end).toMillis();

            System.out.println("Number of matches: " + countOfMatches);
            System.out.println("Search time in milliseconds: " + timeElapsed);
        } finally {
            this.objectStoreFileDownloadExectuor.shutdownNow();
            this.fileSearchExecutor.shutdownNow();
        }
    }
}
