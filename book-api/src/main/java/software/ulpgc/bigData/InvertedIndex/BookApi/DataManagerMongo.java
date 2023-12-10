package software.ulpgc.bigData.InvertedIndex.BookApi;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


public class DataManagerMongo implements Command{
    private final MongoDBReader mongoDBReader;

    public DataManagerMongo() {
        this.mongoDBReader = new MongoDBReader();
    }

    @Override
    public List<Book> getAllBooks() {
        return mongoDBReader.readBooks();
    }

    @Override
    public List<Word> getAllWords() {
        return mongoDBReader.readWords();
    }

    @Override
    public List<Associate> getAllAssociations() {
        return mongoDBReader.readAssociations();
    }



    public HashMap<String, List<Associate>> searchBooks(String query, String author, String from, String to) {
        List<Associate> associatesForKeyword = mongoDBReader.getAssociationsForKeyword(query, author, from, to);

        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        String timestampString = dateFormat.format(now);

        HashMap<String, List<Associate>> hashMapWord = new HashMap<>();
        hashMapWord.put(timestampString, associatesForKeyword);

        return hashMapWord;
    }
}

