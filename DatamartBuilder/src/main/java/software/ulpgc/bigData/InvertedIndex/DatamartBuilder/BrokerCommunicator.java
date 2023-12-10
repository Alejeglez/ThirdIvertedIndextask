package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import javax.jms.Connection;
import javax.jms.JMSException;

public interface BrokerCommunicator {
    Connection createConnection() throws JMSException;
    String receiveMessageFromMetadataQueue(Connection connection) throws JMSException;
    String receiveMessageFromContentQueue(Connection connection) throws JMSException;
}
