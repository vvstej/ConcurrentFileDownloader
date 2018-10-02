package com.scalyr.s3.search.objectstore.client;

public class ObjectStoreFile {

    private byte[] fileContents;

    public ObjectStoreFile(byte[] bytes) {
        this.fileContents = bytes;
    }

    public byte[] getFileContents() {
        return fileContents;
    }

    public void setFileContents(byte[] fileContents) {
        this.fileContents = fileContents;
    }
}
