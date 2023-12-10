package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import com.mongodb.MongoClient;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.io.IOException;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.Map;

public class Controller {
    private final Datalake datalake;
    private final Datamart datamart;
    private final ArtemisCommunicator artemisCommunicator;
    private final MongoDBDatamart mongoDBDatamart;

    public Controller(Datalake datalake, Datamart datamart) {
        this.datalake = datalake;
        this.datamart = datamart;
        this.artemisCommunicator = new ArtemisCommunicator();
        this.mongoDBDatamart = new MongoDBDatamart("mongo");
    }

    public void execute() throws IOException, JMSException {
        Connection connection = artemisCommunicator.createConnection();
        while (true) {
            String fileName = artemisCommunicator.receiveMessageFromContentQueue(connection);
            String metadata = artemisCommunicator.receiveMessageFromMetadataQueue(connection);
            taskForMongoDB(Path.of(fileName), Path.of(metadata));
        }
    }


    private void task(Path filename, Path metadata) throws SQLException, IOException, JMSException {
        taskDelete();
        Map<Book, Map<Associate, Word>> books = datalake.read(filename.toFile(), metadata.toFile(), datamart.getMaxId());
        System.out.println("Filename: ");
        System.out.println(filename.toFile());
        for (Map.Entry<Book, Map<Associate, Word>> entry : books.entrySet()) {
            Book book = entry.getKey();
            Map<Associate, Word> bookData = entry.getValue();
            datamart.addBook(book);

            for (Map.Entry<Associate, Word> dataEntry : bookData.entrySet()) {
                Associate associate = dataEntry.getKey();
                Word word = dataEntry.getValue();
            }
        }
    }

    private void taskDelete() throws SQLException {
        datamart.initDatabase();
    }

    public void taskForMongoDB(Path content, Path metadata) throws IOException, JMSException {
        Map<Book, Map<Associate, Word>> bookMap = datalake.read(content.toFile(), metadata.toFile(), datamart.getMaxId());
        MongoClient client= mongoDBDatamart.createClient();
        mongoDBDatamart.setup(bookMap, client);
        client.close();
    }
}
