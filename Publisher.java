package yellowcab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.stream.Stream;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Publisher implements Runnable {

    @Override
    public void run() {

        try {

            // Administrative object
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create session object
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Administrative object
            Destination dest = session.createTopic("myTopic");
            
            // Producer object
            MessageProducer producer = session.createProducer(dest);
            
            // Avoid disk writing by setting Delivery Mode to NON_PERSISTENT
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            
            /* Disable Message ID and Time Stamp creation for each message
                at producer end */
            producer.setDisableMessageID(true);
            producer.setDisableMessageTimestamp(true);

            String fileName = "SortedTaxiData.csv";
            
            // Stream Sorted CSV row by row
            try (Stream<String> data = Files.lines(Paths.get(fileName)).filter(s -> !s.isEmpty())) {
                data.skip(1).map(s -> new AbstractMap.SimpleEntry<>(s, s.split(",")))
                        // Filter by only Jan 2018 records
                        .filter(s -> (s.getValue()[1].split(" ")[0].split("-")[1].equals("01")
                        && s.getValue()[1].split(" ")[0].split("-")[0].equals("2018")))
                        .forEach(s -> {
                            try {
                                // Fetch Pickup DateTime and PickUp Location and create message
                                Message message = session.createTextMessage(s.getValue()[1] + "," + s.getValue()[7]);
                                // Send Message to ActiveMQ 
                                producer.send(message);
                            } catch (JMSException e) {
                            }
                        });
            } catch (IOException ex) {
            }
            // Send End OF File Message to ActiveMQ
            // This will help to close the session once the file is processed
            producer.send(session.createTextMessage("eof"));   
            connection.close();
        } catch (JMSException e) {
        }
    }
}
