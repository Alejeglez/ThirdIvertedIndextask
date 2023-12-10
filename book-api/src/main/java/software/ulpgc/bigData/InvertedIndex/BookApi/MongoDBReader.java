package software.ulpgc.bigData.InvertedIndex.BookApi;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MongoDBReader implements DatabaseReader {

    private final MongoClient mongoClient;
    private final MongoDatabase searchDB;

    public MongoDBReader() {
        this.mongoClient = MongoClients.create("mongodb://localhost:27017/");
        this.searchDB = mongoClient.getDatabase("datamart");
    }

    public List<Book> readBooks() {
        Set<Book> uniqueBooks = new HashSet<>();
        MongoCollection<Document> wordsCollection = searchDB.getCollection("words");

        FindIterable<Document> allWordsDocument = wordsCollection.find();

        for (Document word : allWordsDocument) {
            List<Document> booksArray = word.getList("books", Document.class);

            for (Document bookDoc : booksArray) {
                int bookId = (int) bookDoc.get("GutenbergBookId");
                String author = bookDoc.getString("author");
                String title = bookDoc.getString("title");
                String releaseDate = bookDoc.getString("releaseDate");

                Book bookObject = new Book(bookId, author, title, releaseDate);
                uniqueBooks.add(bookObject);
            }
        }

        return new ArrayList<>(uniqueBooks);
    }


    @Override
    public List<Associate> readAssociations() {
        List<Associate> allAssociates = new ArrayList<>();
        MongoCollection<Document> wordsCollection = searchDB.getCollection("words");
        FindIterable<Document> allWordsDocument = wordsCollection.find();
        for (Document word : allWordsDocument) {
            String wordId = word.getString("_id");
            Word wordObject = new Word(wordId);

            List<Document> booksArray = word.getList("books", Document.class);
            for (Document bookDoc : booksArray) {
                int bookId = (int) bookDoc.get("GutenbergBookId");
                String author = bookDoc.getString("author");
                String title = bookDoc.getString("title");
                int count = (int) bookDoc.get("count");
                String releaseDate = bookDoc.getString("releaseDate");

                Book bookObject = new Book(bookId, author, title, releaseDate);
                allAssociates.add(new Associate(wordObject, bookObject, count));
            }
        }
        return allAssociates;
    }

    @Override
    public List<Word> readWords() {
        List<Word> allWords = new ArrayList<>();
        MongoCollection<Document> allWordsCollection = searchDB.getCollection("words");

        FindIterable<Document> allWordsDocument = allWordsCollection.find();

        for (Document word : allWordsDocument) {
            String wordId = (String) word.get("_id");
            Word wordObject = new Word(wordId);
            allWords.add(wordObject);
        }

        return allWords;
    }

    public List<Associate> getAssociationsForKeyword(String keyword, String author, String from, String to) {
        List<Associate> associationsForKeyword = new ArrayList<>();

        MongoCollection<Document> wordsCollection = searchDB.getCollection("words");

        Document wordDocument = wordsCollection.find(Filters.eq("_id", keyword)).first();

        if (wordDocument != null) {
            String wordId = wordDocument.get("_id").toString();
            Word wordObject = new Word(wordId);

            List<Document> associations = wordDocument.getList("books", Document.class);

            for (Document assocDoc : associations) {
                int bookId = (int) assocDoc.get("GutenbergBookId");
                String authorBook = assocDoc.getString("author");
                String title = assocDoc.getString("title");
                int count = (int) assocDoc.get("count");
                String releaseDate = assocDoc.getString("releaseDate");
                if ((author == null || authorBook.equals(author)) && isBookInDateRange(releaseDate, from, to)) {
                    Book bookObject = new Book(bookId, authorBook, title, releaseDate);
                    Associate association = new Associate(wordObject, bookObject, count);
                    associationsForKeyword.add(association);
                }
            }
        }

        return associationsForKeyword;
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

    public void closeConnection() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
