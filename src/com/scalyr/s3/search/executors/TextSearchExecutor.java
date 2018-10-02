package com.scalyr.s3.search.executors;

import com.scalyr.s3.search.objectstore.client.ObjectStoreFile;
import com.scalyr.s3search.textsearch.SearchResult;
import com.scalyr.s3search.textsearch.TextSearcher;

public class TextSearchExecutor {

    final TextSearcher searcher;
    final String searchText;
    final ObjectStoreFile fileToSearch;

    public TextSearchExecutor(final TextSearcher searcher, final String searchText, ObjectStoreFile file) {
        this.searcher = searcher;
        this.searchText = searchText;
        this.fileToSearch = file;
    }

    public SearchResult searchFile() {
        if (fileToSearch == null)
            return new SearchResult(0);
        int matches =
                searcher.countMatchesInBlob(fileToSearch.getFileContents(), 0, fileToSearch.getFileContents().length);
        return new SearchResult(matches);
    }

}
