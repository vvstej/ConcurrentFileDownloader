package com.scalyr.s3.search.executors;

import java.util.Optional;
import java.util.concurrent.Callable;

import com.scalyr.s3.search.objectstore.client.ObjectStoreClient;
import com.scalyr.s3.search.objectstore.client.ObjectStoreFile;
import com.scalyr.s3.search.objectstore.client.SimulatedS3Client.FlakyNetworkException;

public class FileDownloadExecutor {
    private static final int RETRY_ATTEMPTS = 5;
    final ObjectStoreClient client;
    final String bucketName;
    final String fileName;

    public FileDownloadExecutor(final ObjectStoreClient client, final String bucketName, final String fileName) {
        this.client = client;
        this.bucketName = bucketName;
        this.fileName = fileName;
    }

    public ObjectStoreFile call() {
        int attempt = 0;
        byte[] bytes = null;
        while (attempt <= RETRY_ATTEMPTS) {
            attempt++;
            try {
                bytes = this.client.downloadFile(this.bucketName, this.fileName);
                break;
            } catch (FlakyNetworkException e) {
                // log exception

            }
        }
        if (bytes == null)
            return null;
        return new ObjectStoreFile(bytes);
    }

}
