package software.ulpgc.bigData.InvertedIndex.crawler;

import java.io.IOException;

public interface BookReader {
    String read(String bookID, String type) throws IOException;
    void downloadFile(String fileURL) throws IOException;

}
