package com.scalyr.s3.search.objectstore.client;

import com.scalyr.s3.search.objectstore.client.SimulatedS3Client.FlakyNetworkException;

public interface ObjectStoreClient {

    byte[] downloadFile(final String bucketName, final String fileName) throws FlakyNetworkException;
}
