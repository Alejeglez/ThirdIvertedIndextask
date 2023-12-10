package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import javax.jms.JMSException;
import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException, IOException, InterruptedException, JMSException {
        Controller controller = new Controller(new FileDatalake(), new DatabaseWriter());
        controller.execute();
    }
}