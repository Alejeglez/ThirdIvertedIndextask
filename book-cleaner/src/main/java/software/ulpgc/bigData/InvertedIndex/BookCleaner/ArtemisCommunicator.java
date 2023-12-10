package software.ulpgc.bigData.InvertedIndex.BookCleaner;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;

public class ArtemisCommunicator implements BrokerCommunicator{
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String QUEUE_NAME = "DownloadedBooks";
    private static final String QUEUE_NAME_TO_SEND = "Metadata";
    private static final String QUEUE_NAME_TO_SEND2 = "Content";
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
        createQueueIfNotExists(connection);
        return connection;
    }

    @Override
    public String receiveMessageFromQueue(Connection connection) throws JMSException {
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

    private void createQueueIfNotExists(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(ArtemisCommunicator.QUEUE_NAME);
        session.close();
    }

    @Override
    public void sendMessageToQueue(Connection connection, String text,String type) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (type.equals("metadata")){
            Destination destination = session.createQueue(QUEUE_NAME_TO_SEND);
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage(text);
            producer.send(message);
        }else{
            Destination destination = session.createQueue(QUEUE_NAME_TO_SEND2);
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage(text);
            producer.send(message);
        }
        session.close();
    }
}
