package software.ulpgc.bigData.InvertedIndex.BookCleaner;

import javax.jms.Connection;
import javax.jms.JMSException;

public interface BrokerCommunicator {
    void sendMessageToQueue(Connection connection, String message, String type)  throws JMSException;
    Connection createConnection() throws JMSException;
    String receiveMessageFromQueue(Connection connection) throws JMSException;
}
