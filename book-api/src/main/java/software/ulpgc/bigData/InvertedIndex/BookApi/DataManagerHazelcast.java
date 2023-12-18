package software.ulpgc.bigData.InvertedIndex.BookApi;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DataManagerHazelcast {
    private final HazelCastReader hazelcastReader;

    public DataManagerHazelcast() {
        this.hazelcastReader = new HazelCastReader();
    }


    public HashMap<String, List<Associate>>  searchBooks(String query, String author, String from, String to) {
        List<Associate> associatesForKeyword = hazelcastReader.getAssociationsForKeyword(query, author, from, to);
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        String timestampString = dateFormat.format(now);

        HashMap<String, List<Associate>> hashMapWord = new HashMap<>();
        hashMapWord.put(timestampString, associatesForKeyword);

        return hashMapWord;
    }

    public HashMap<String, Integer> getBooksWordsCount(){
        return hazelcastReader.getBooksWordsCount();
    }
}
