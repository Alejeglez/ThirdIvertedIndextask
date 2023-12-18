package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

public class MongoDBAtlasDatamart {
    private final MongoClientURI uri;
    private final String DATABASE_NAME = "datamart";
    private final String COLLECTION_BOOKS = "books";
    private final String COLLECTION_WORDS = "words";
    private Set<String> existingWordsSet = new HashSet<>();
    public MongoDBAtlasDatamart() {
        uri = new MongoClientURI("mongodb+srv://bigdata:InvertedIndex@worddatamartcluster.3hihtyt.mongodb.net/?retryWrites=true&w=majority");
    }

    public void setup(Map<Book, Map<Associate, Word>> bookMap, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collectionWords = database.getCollection(COLLECTION_WORDS);
        MongoCollection<Document> collectionBooks = database.getCollection(COLLECTION_BOOKS);

        Book book = bookMap.keySet().iterator().next();

        Map<Associate, Word> associates = bookMap.get(book);

        insertDocumentBook(collectionBooks, book);

        insertDocumentsAssociation(collectionWords, book, associates);
    }

    private void insertDocumentBook(MongoCollection<Document> booksCollection, Book book){
        int bookId = book.getId();

        Document existingBook = booksCollection.find(new Document("_id", bookId)).first();

        if (existingBook == null) {
            String title = book.getTitle();
            String author = book.getAuthor();
            String releaseDate = book.getReleaseYear();

            Document bookDocument = new Document("_id", bookId)
                    .append("title", title)
                    .append("author", author)
                    .append("releaseDate", releaseDate);

            booksCollection.insertOne(bookDocument);
        }
    }

    private void insertOrUpdateDocumentAssociation(MongoCollection<Document> collection, int bookId, int wordCount, String wordLabel) {
        Document updateDocument = new Document("$set", new Document("_id", wordLabel));

        updateDocument.append("$set", new Document("books." + bookId, wordCount));

        collection.updateOne(new Document("_id", wordLabel), updateDocument);
    }

    private void insertDocumentsAssociation(MongoCollection<Document> collection, Book book, Map<Associate, Word> associates) {
        List<Document> documentsToInsert = new ArrayList<>();
        List<UpdateDocument> updates = new ArrayList<>();

        for (Map.Entry<Associate, Word> associateEntry : associates.entrySet()) {
            Associate associate = associateEntry.getKey();
            Word word = associateEntry.getValue();

            String wordLabel = word.getLabel();
            int wordCount = associate.getCount();
            int bookId = book.getId();

            if (existingWordsSet.contains(wordLabel)) {
                Bson update = Updates.combine(
                        Updates.set("_id", wordLabel),
                        Updates.set("books." + bookId, wordCount)
                );
                updates.add(new UpdateDocument(wordLabel, update));
            }else {
                existingWordsSet.add(wordLabel);
                Document documentToInsert = new Document("_id", wordLabel)
                        .append("books", new Document(String.valueOf(bookId), wordCount));
                documentsToInsert.add(documentToInsert);
            }
        }



        if (!documentsToInsert.isEmpty()) {
            InsertManyOptions insertManyOptions = new InsertManyOptions().ordered(false);
            collection.insertMany(documentsToInsert, insertManyOptions);
        }
        if (!updates.isEmpty()){
            insertOrUpdateDocumentsAssociation(collection, updates);
        }
    }


    private void insertOrUpdateDocumentsAssociation(MongoCollection<Document> collection, List<UpdateDocument> updates) {
        List<WriteModel<Document>> writeModels = new ArrayList<>();

        for (UpdateDocument updateDocument : updates) {
            Bson filter = Filters.eq("_id", updateDocument.getWordLabel());
            Bson update = updateDocument.getUpdate();

            UpdateOptions updateOptions = new UpdateOptions().upsert(true);
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter, update, updateOptions);
            writeModels.add(updateOneModel);
        }

        if (!writeModels.isEmpty()) {
            BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
            collection.bulkWrite(writeModels, bulkWriteOptions);
        }
    }

    public MongoClient createClient() {
        MongoClient mongoClient = new MongoClient(uri);
        return mongoClient;
    }
}
