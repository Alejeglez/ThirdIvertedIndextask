package software.ulpgc.bigData.InvertedIndex.BookApi;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.*;

public class MongoDBAtlasReader implements DatabaseReader {

    private final MongoClient mongoClient;
    private final MongoDatabase searchDB;

    public MongoDBAtlasReader() {
        this.mongoClient = MongoClients.create("mongodb+srv://bigdata:InvertedIndex@worddatamartcluster.3hihtyt.mongodb.net/?retryWrites=true&w=majority");
        this.searchDB = mongoClient.getDatabase("datamart");
    }

    public List<Book> readBooks() {
        Set<Book> uniqueBooks = new HashSet<>();
        MongoCollection<Document> wordsCollection = searchDB.getCollection("books");

        FindIterable<Document> allBooksDocument = wordsCollection.find();

        for (Document bookDoc : allBooksDocument) {
            int bookId = (int) bookDoc.get("_id");
            String author = bookDoc.getString("author");
            String title = bookDoc.getString("title");
            String releaseDate = bookDoc.getString("releaseDate");

            Book bookObject = new Book(bookId, author, title, releaseDate);
            uniqueBooks.add(bookObject);
        }

        return new ArrayList<>(uniqueBooks);
    }


    @Override
    public List<Associate> readAssociations() {
        List<Associate> allAssociates = new ArrayList<>();
        MongoCollection<Document> wordsCollection = searchDB.getCollection("words");
        MongoCollection<Document> booksCollection = searchDB.getCollection("books");
        FindIterable<Document> allWordsDocument = wordsCollection.find();
        for (Document word : allWordsDocument) {
            String wordId = word.getString("_id");
            Word wordObject = new Word(wordId);

            Document bookDocument = (Document) word.get("books");

            for (Map.Entry<String, Object> entry : bookDocument.entrySet()) {
                Associate association = getAssociation(entry, booksCollection, wordObject);
                allAssociates.add(association);
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
        MongoCollection<Document> booksCollection = searchDB.getCollection("books");

        Document wordDocument = wordsCollection.find(Filters.eq("_id", keyword)).first();
        if (wordDocument != null) {
            String wordId = wordDocument.getString("_id");
            Word wordObject = new Word(wordId);
            Document bookDocument = (Document) wordDocument.get("books");

            for (Map.Entry<String, Object> entry : bookDocument.entrySet()) {
                Associate associate = getAssociation(entry, booksCollection, wordObject);
                String authorBook = associate.getBook().getAuthor();
                String releaseDate = associate.getBook().getReleaseYear();
                if ((author == null || authorBook.equals(author)) && isBookInDateRange(releaseDate, from, to)) {
                    associationsForKeyword.add(associate);
                }
            }
        }

        return associationsForKeyword;
    }

    public HashMap<String, Integer> getWordsCountByBook() {
        HashMap<String, Integer> wordsCountByBook = new HashMap<>();
        MongoCollection<Document> wordsCollection = searchDB.getCollection("words");
        MongoCollection<Document> booksCollection = searchDB.getCollection("books");

        FindIterable<Document> allWordsDocument = wordsCollection.find();

        for (Document word : allWordsDocument) {
            String wordId = word.getString("_id");
            Word wordObject = new Word(wordId);

            Document bookDocument = (Document) word.get("books");

            for (Map.Entry<String, Object> entry : bookDocument.entrySet()) {
                Associate association = getAssociation(entry, booksCollection, wordObject);
                String bookTitle = association.getBook().getTitle();

                wordsCountByBook.merge(bookTitle, association.getCount(), Integer::sum);
            }
        }
        return wordsCountByBook;
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

    private static Associate getAssociation(Map.Entry<String, Object> entry, MongoCollection<Document> booksCollection, Word wordObject) {
        Document bookDocument;
        int count = (int) entry.getValue();
        int bookIdInt = Integer.parseInt(entry.getKey());
        bookDocument = booksCollection.find(Filters.eq("_id", bookIdInt)).first();

        String authorBook = bookDocument.getString("author");
        String title = bookDocument.getString("title");
        String releaseDate = bookDocument.getString("releaseDate");

        Book bookObject = new Book(bookIdInt, authorBook, title, releaseDate);
        return new Associate(wordObject, bookObject, count);
    }

    public void closeConnection() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
