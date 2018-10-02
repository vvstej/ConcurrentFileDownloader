package com.scalyr.s3.search.executors;

import com.scalyr.s3.search.objectstore.client.ObjectStoreClient;
import com.scalyr.s3.search.objectstore.client.ObjectStoreFile;
import com.scalyr.s3.search.objectstore.client.SimulatedS3Client.FlakyNetworkException;

public class FileDownloadExecutor {
    private static final int RETRY_ATTEMPTS = 5;
    private static final int INIT_WAIT = 500;
    final ObjectStoreClient client;
    final String bucketName;
    final String fileName;

    public FileDownloadExecutor(final ObjectStoreClient client, final String bucketName, final String fileName) {
        this.client = client;
        this.bucketName = bucketName;
        this.fileName = fileName;
    }

    public ObjectStoreFile download() {
        int attempt = 0;
        byte[] bytes = null;
        int power = 0;
        while (attempt <= RETRY_ATTEMPTS) {
            attempt++;
            try {
                bytes = this.client.downloadFile(this.bucketName, this.fileName);
                break;
            } catch (FlakyNetworkException e) {
                System.out.println("Attempt to download failed, retrying after a while..");
                try {
                    Thread.sleep(INIT_WAIT * (long)Math.pow(2.0, power));
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                finally {
                    power++;
                }

            }
        }
        if (bytes == null)
            return null;
        return new ObjectStoreFile(bytes);
    }

}
