package com.scalyr.s3search.textsearch;

public class SearchResult {

    final int numberOfMatches;

    public int getNumberOfMatches() {
        return numberOfMatches;
    }

    public SearchResult(final int numberOfMatches) {
        this.numberOfMatches = numberOfMatches;
    }
}
