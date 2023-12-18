package software.ulpgc.bigData.InvertedIndex.BookCleaner;

import java.io.IOException;

public interface DocumentSplitter {
    void splitDocument(String path) throws IOException, InterruptedException;
    String prepareMetadata(String text);

    void storeFile(String path, String content, String type);

}
