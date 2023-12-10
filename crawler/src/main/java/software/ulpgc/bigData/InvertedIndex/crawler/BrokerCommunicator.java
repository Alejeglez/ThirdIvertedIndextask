package software.ulpgc.bigData.InvertedIndex.crawler;

import javax.jms.Connection;
import javax.jms.JMSException;

public interface BrokerCommunicator {
    void sendMessageToQueue(Connection connection, String message)  throws JMSException;
    Connection createConnection() throws JMSException;
}
