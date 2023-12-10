package software.ulpgc.bigData.InvertedIndex.BookApi;



import java.util.List;

public interface DatabaseReader {
    List<Book> readBooks();

    List<Word> readWords();

    List<Associate> readAssociations();
}
