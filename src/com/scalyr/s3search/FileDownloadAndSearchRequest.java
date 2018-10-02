package com.scalyr.s3search;

import com.scalyr.s3.search.objectstore.client.ObjectStoreClient;

public class FileDownloadAndSearchRequest {

    final ObjectStoreClient remoteObjectStoreClient;
    final String bucketName;
    final int startEpoch;
    final int endEpoch;

    public FileDownloadAndSearchRequest(final ObjectStoreClient remoteObjectStoreClient, final String bucketName,
            final int startEpoch, final int endEpoch) {
        this.remoteObjectStoreClient = remoteObjectStoreClient;
        this.bucketName = bucketName;
        this.startEpoch = startEpoch;
        this.endEpoch = endEpoch;
    }

    public ObjectStoreClient getRemoteObjectStoreClient() {
        return remoteObjectStoreClient;
    }

    public String getBucketName() {
        return bucketName;
    }

    public int getStartEpoch() {
        return startEpoch;
    }

    public int getEndEpoch() {
        return endEpoch;
    }
}
