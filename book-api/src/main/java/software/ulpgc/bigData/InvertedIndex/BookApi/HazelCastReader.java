package software.ulpgc.bigData.InvertedIndex.BookApi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.org.json.JSONObject;

import java.util.*;

public class HazelCastReader implements DatabaseReader {

    private HazelcastInstance hazelcastInstance;

    public Config getConfig(){
        Config config = new Config();
        config.setClusterName("datamartCluster");

        return config;
    }
    public HazelCastReader() {
        this.hazelcastInstance = Hazelcast.newHazelcastInstance(getConfig());
    }


    @Override
    public List<Book> readBooks() {
        Set<Book> uniqueBooks = new HashSet<>();
        MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("datamart");

        Collection<String> jsonValues = multiMap.values();

        for (String json : jsonValues) {
            JSONObject jsonObject = new JSONObject(json);
            String bookTitle = jsonObject.getString("bookTitle");
            String authorBook = jsonObject.getString("author");
            int gutenbergBookId = jsonObject.getInt("GutenbergBookId");
            String releaseDate = jsonObject.getString("releaseDate");
            Book book = new Book(gutenbergBookId, authorBook, bookTitle, releaseDate);

            uniqueBooks.add(book);
        }
        return new ArrayList<>(uniqueBooks);
    }
    public List<Word> readWords() {
        List<Word> words = new ArrayList<>();
        MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("datamart");

        for (String wordId : multiMap.keySet()) {
            words.add(new Word(wordId));
        }

        return words;
    }

    @Override
    public List<Associate> readAssociations() {
        List<Associate> allAssociations = new ArrayList<>();
        MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("datamart");

        for (Map.Entry<String, String> entry : multiMap.entrySet()) {
            String wordLabel = entry.getKey();
            Collection<String> values = Collections.singleton(entry.getValue());

            Word wordObject = new Word(wordLabel);
            for (String json : values) {
                Associate association = getAssociation(json, wordObject);
                allAssociations.add(association);
            }
        }

        return allAssociations;
    }

    public List<Associate> getAssociationsForKeyword(String wordLabel, String author, String from, String to) {
        List<Associate> associationsForKeyword = new ArrayList<>();
        MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("datamart");

        Collection<String> values = multiMap.get(wordLabel);
        Word wordObject = new Word(wordLabel);
        for (String json : values) {
            Associate association = getAssociation(json, wordObject);

            String authorBook = association.getBook().getAuthor();
            String releaseDate = association.getBook().getReleaseYear();

            if ((author == null || authorBook.equals(author)) && isBookInDateRange(releaseDate, from, to)) {
                associationsForKeyword.add(association);
            }
        }

        return associationsForKeyword;
    }

    public HashMap<String, Integer> getBooksWordsCount() {
        HashMap<String, Integer> booksWordCount = new HashMap<>();
        MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("datamart");

        for (String wordLabel : multiMap.keySet()) {
            Collection<String> values = multiMap.get(wordLabel);
            for (String json : values) {
                Associate association = getAssociation(json, new Word(wordLabel));
                String bookTitle = association.getBook().getTitle();

                booksWordCount.merge(bookTitle, association.getCount(), Integer::sum);
            }
        }
        return booksWordCount;
    }

    private static Associate getAssociation(String json, Word wordObject) {
        JSONObject jsonObject = new JSONObject(json);
        String bookTitle = jsonObject.getString("bookTitle");
        String authorBook = jsonObject.getString("author");
        int wordOccurrences = jsonObject.getInt("wordOccurrences");
        int gutenbergBookId = jsonObject.getInt("GutenbergBookId");
        String releaseDate = jsonObject.getString("releaseDate");

        Book bookObject = new Book(gutenbergBookId, authorBook, bookTitle, releaseDate);
        return new Associate(wordObject, bookObject, wordOccurrences);
    }
    private boolean isBookInDateRange(String releaseDate, String from, String to) {
        if (from == null && to == null) {
            return true;
        }

        if (from == null) {
            return isBeforeDate(releaseDate, to);
        }

        if (to == null) {
            return isAfterDate(releaseDate, from);
        }

        return isAfterDate(releaseDate, from) && isBeforeDate(releaseDate, to);
    }

    private boolean isAfterDate(String releaseDate, String from) {
        int releaseYearInt = Integer.parseInt(releaseDate);
        int fromYearInt = Integer.parseInt(from);
        return releaseYearInt >= fromYearInt;
    }

    private boolean isBeforeDate(String releaseYear, String toYear) {
        int releaseYearInt = Integer.parseInt(releaseYear);
        int toYearInt = Integer.parseInt(toYear);
        return releaseYearInt <= toYearInt;
    }
}
