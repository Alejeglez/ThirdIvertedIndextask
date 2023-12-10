package software.ulpgc.bigData.InvertedIndex.BookApi;

import java.sql.SQLException;

public class Controller {
    private final APISource api;
    private final MongoDBReader dbReader;

    public Controller(APISource api) throws SQLException {
        this.api = api;
        this.dbReader = new MongoDBReader();
    }

    public void start() {
        api.startServer();
        api.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }


    public void stop() {
        try {
            api.stopServer();
        } finally {
            dbReader.closeConnection();
        }
    }
}
