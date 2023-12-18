package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.mongodb.MongoClient;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.io.IOException;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Controller {
    private final Datalake datalake;
    private final Datamart datamart;
    private final ArtemisCommunicator artemisCommunicator;
    private MongoDBAtlasDatamart mongoDBAtlasDatamart;
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private final HazelcastDatamart hazelcastDatamart;
    private final Config config = new Config();
    public Controller(Datalake datalake, Datamart datamart) {
        this.datalake = datalake;
        this.datamart = datamart;
        this.artemisCommunicator = new ArtemisCommunicator();
        this.mongoDBAtlasDatamart = new MongoDBAtlasDatamart();
        config.setClusterName("datamartCluster");
        this.hazelcastDatamart = new HazelcastDatamart(Hazelcast.newHazelcastInstance(config));
    }

    public void executeArtemis() throws IOException, JMSException {
        MongoClient client = mongoDBAtlasDatamart.createClient();
        Connection connection = artemisCommunicator.createConnection();
        try {
            while (true) {
                String fileName = artemisCommunicator.receiveMessageFromContentQueue(connection);
                String metadata = artemisCommunicator.receiveMessageFromMetadataQueue(connection);
                taskForMongoDBAtlas(Path.of(fileName), Path.of(metadata), client);
            }
        } finally {
            client.close();
        }
    }


    public void executeMongoDBAtlas() throws IOException, JMSException {
        Path datalakePath = Paths.get("datalake");
        MongoClient client = mongoDBAtlasDatamart.createClient();
        try (DirectoryStream<Path> dataLakeStream = Files.newDirectoryStream(datalakePath)) {
            for (Path folderDay : dataLakeStream) {
                if (Files.isDirectory(folderDay)) {
                    try (DirectoryStream<Path> folderBookStream = Files.newDirectoryStream(folderDay)) {
                        for (Path folderBook : folderBookStream) {
                            if (Files.isDirectory(folderBook )) {
                                String id = folderBook .toString().substring(18);
                                Path contentFile = folderBook .resolve(id + "_content.txt");
                                Path metadataFile = folderBook .resolve(id + "_metadata.json");
                                System.out.println("The book (Metodo Anterior):" + id);

                                long startTime = System.currentTimeMillis();
                                taskForMongoDBAtlas(contentFile, metadataFile, client);

                                long endTime = System.currentTimeMillis();

                                long elapsedTime = endTime - startTime;
                                System.out.println("Time elapsed for book " + id + ": " + elapsedTime + " milliseconds");
                            }
                        }
                    }
                }
            }
        }
        finally {
            client.close();
        }
    }
    public void executeHazelcast() throws IOException{
        Path datalakePath = Paths.get("datalake");

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        try {
            try (DirectoryStream<Path> dataLakeStream = Files.newDirectoryStream(datalakePath)) {
                for (Path folderDay : dataLakeStream) {
                    if (Files.isDirectory(folderDay)) {
                        executor.execute(() -> processDayFolder(folderDay));
                    }
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    private void processDayFolder(Path folderDay) {
        try (DirectoryStream<Path> folderBookStream = Files.newDirectoryStream(folderDay)) {
            for (Path folderBook : folderBookStream) {
                processBookFolder(folderBook);
            }
        } catch (JMSException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processBookFolder(Path folderBook) throws JMSException, IOException {
        if (Files.isDirectory(folderBook)) {
            System.out.println(hazelcastDatamart.getMultimapName());
            String id = folderBook.toString().substring(18);
            Path contentFile = folderBook.resolve(id + "_content.txt");
            Path metadataFile = folderBook.resolve(id + "_metadata.json");
            System.out.println("The book (hazelcast):" + id);
            long startTime = System.currentTimeMillis();
            taskforHazelcast(contentFile, metadataFile);
            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime;
            System.out.println("Time elapsed for book " + id + ": " + elapsedTime + " milliseconds");
            System.out.println();
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

    private void taskforHazelcast(Path contentFile, Path metadataFile) throws JMSException, IOException {
        Map<Book, Map<Associate, Word>> bookMap = datalake.read(contentFile.toFile(), metadataFile.toFile(), datamart.getMaxId());
        hazelcastDatamart.setup(bookMap);
    }


    public void taskForMongoDBAtlas(Path content, Path metadata, MongoClient client) throws JMSException, IOException {
        Map<Book, Map<Associate, Word>> bookMap = datalake.read(content.toFile(), metadata.toFile(), datamart.getMaxId());
        mongoDBAtlasDatamart.setup(bookMap, client);
    }
}
