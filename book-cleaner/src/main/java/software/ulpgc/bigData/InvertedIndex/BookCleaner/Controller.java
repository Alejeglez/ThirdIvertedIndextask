package software.ulpgc.bigData.InvertedIndex.BookCleaner;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class Controller {
    private final GutenbergSplitter splitter;
    private final MissingFilesManagement missingFilesManagement;
    private final SimpleDateFormat dateFormat;
    private final software.ulpgc.bigData.InvertedIndex.BookCleaner.ArtemisCommunicator artemisCommunicator;

    public Controller() {
        this.splitter = new GutenbergSplitter();
        this.missingFilesManagement = new MissingFilesManagement();
        this.dateFormat = new SimpleDateFormat("yyyyMMdd");
        artemisCommunicator = new ArtemisCommunicator();
    }

    public void execute() throws JMSException {
        Connection connection = artemisCommunicator.createConnection();
        while(true) {
            try {
                String fileName = artemisCommunicator.receiveMessageFromQueue(connection);
                String[] parts = fileName.split("//");
                int index = parts.length - 2;
                String id = parts[index];
                splitter.splitDocument(id);
            } catch (JMSException | IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}