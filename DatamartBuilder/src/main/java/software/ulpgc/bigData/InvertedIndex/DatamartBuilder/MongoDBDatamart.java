package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MongoDBDatamart {
    private final String containerName;
    private static Document wordDocument;
    private static Document bookDocument;
    private final MongoClientURI uri;
    private final String DATABASE_NAME = "datamart";
    private final String COLLECTION_NAME = "words";//words

    public MongoDBDatamart(String containerName) {
        this.containerName = containerName;
        wordDocument = null;
        bookDocument = null;
        uri = new MongoClientURI("mongodb://localhost:27017");
    }

    public void setup(Map<Book, Map<Associate, Word>> bookMap, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        Book book = bookMap.keySet().iterator().next();
        Map<Associate, Word> associates = bookMap.values().iterator().next();

        for (Map.Entry<Associate, Word> associateEntry : associates.entrySet()) {
            Associate associate = associateEntry.getKey();
            Word word = associateEntry.getValue();

            insertOrUpdateDocument(collection, book, associate, word);
        }
    }

    private void insertOrUpdateDocument(MongoCollection<Document> collection, Book book, Associate associate, Word word) {
        String wordLabel = word.getLabel();
        String title = book.getTitle();
        String author = book.getAuthor();
        int wordCount = associate.getCount();
        int bookId = book.getId();
        String releaseDate = book.getReleaseYear();

        Document bookDocument = new Document("title", title)
                .append("author", author)
                .append("count", wordCount)
                .append("GutenbergBookId", bookId)
                .append("releaseDate", releaseDate);

        Document existingWord = collection.find(new Document("_id", wordLabel)).first();

        if (existingWord != null) {
            boolean bookExists = isBookInList(existingWord, bookDocument);

            if (!bookExists) {
                collection.updateOne(
                        new Document("_id", wordLabel),
                        new Document("$push", new Document("books", bookDocument))
                );
            } else {
                System.out.println("The book already exists for the word : " + wordLabel);
            }
        } else {
            Document wordDocument = new Document("_id", wordLabel)
                    .append("books", Arrays.asList(bookDocument));
            collection.insertOne(wordDocument);
        }
    }

    private boolean isBookInList(Document wordDocument, Document bookDocument) {
        List<Document> booksList = (List<Document>) wordDocument.get("books");
        for (Document doc : booksList) {
            if (doc.getString("title").equals(bookDocument.getString("title")) &&
                    doc.getString("author").equals(bookDocument.getString("author")) &&
                    doc.getInteger("count").equals(bookDocument.getInteger("count")) &&
                    doc.getInteger("GutenbergBookId").equals(bookDocument.getInteger("GutenbergBookId")) &&
                    doc.getString("releaseDate").equals(bookDocument.getString("releaseDate"))) {
                return true;
            }
        }
        return false;
    }

    public MongoClient createClient() {
        MongoClient mongoClient = new MongoClient(uri);
        return mongoClient;
    }
}
