package software.ulpgc.bigData.InvertedIndex.BookApi;

import java.sql.SQLException;

public class Controller {
    private final APISource api;

    public Controller(APISource api) throws SQLException {
        this.api = api;
    }

    public void start() {
        api.startServer();
        api.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }


    public void stop() {
        try {
            api.stopServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
