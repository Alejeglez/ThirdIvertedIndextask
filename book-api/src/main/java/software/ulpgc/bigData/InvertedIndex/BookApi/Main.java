package software.ulpgc.bigData.InvertedIndex.BookApi;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        //Controller controller = new Controller(new SparkWebServiceMongoDBAtlas());
        Controller controller = new Controller(new SparkWebServiceHazelcast());
        Runtime.getRuntime().addShutdownHook(new Thread(controller::stop));

        controller.start();
    }
}
