package software.ulpgc.bigData.InvertedIndex.BookCleaner;

import java.util.List;

public interface BookCleaner {
    String cleanText(String content);

    List<String> readWords(String path);
}
