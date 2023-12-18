package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;

public class ArtemisCommunicator implements BrokerCommunicator {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String QUEUE_NAME = "Metadata";
    private static final String QUEUE_NAME2 = "Content";
    private static final String USERNAME = "artemis";
    private static final String PASSWORD = "artemis";

    @Override
    public Connection createConnection() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(BROKER_URL);
        connectionFactory.setUser(USERNAME);
        connectionFactory.setPassword(PASSWORD);
        Connection connection = null;
        connection = connectionFactory.createConnection();
        connection.start();
        synchronizeContentandMetadataQueues(connection);
        return connection;
    }

    @Override
    public String receiveMessageFromMetadataQueue(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME);
        MessageConsumer consumer = session.createConsumer(destination);

        Message message = consumer.receive();
        String text = null;
        if (message instanceof TextMessage) {
            text = ((TextMessage) message).getText();
        }

        consumer.close();
        session.close();

        return text;
    }

    @Override
    public String receiveMessageFromContentQueue(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(QUEUE_NAME2);
        MessageConsumer consumer = session.createConsumer(destination);

        Message message = consumer.receive();
        String text = null;
        if (message instanceof TextMessage) {
            text = ((TextMessage) message).getText();
        }

        consumer.close();
        session.close();

        return text;
    }

    private void createQueueIfNotExists(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(ArtemisCommunicator.QUEUE_NAME);
        session.close();
    }
    public void synchronizeContentandMetadataQueues(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(ArtemisCommunicator.QUEUE_NAME);
        Queue queue2 = session.createQueue(ArtemisCommunicator.QUEUE_NAME2);
        session.close();
    }
}
