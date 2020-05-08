package yellowcab;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

public class ConsumerFactory implements Runnable {

    @Override
    public void run() {

        try {
            // Administrative object
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

            Connection connection = connectionFactory.createConnection();

            // Create session object
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Administrative object
            Destination dest = session.createTopic("myTopic");
            
            // Consumer1 subscribes to customerTopic
            MessageConsumer consumer1 = session.createConsumer(dest);
            consumer1.setMessageListener(new Consumer1());
             
            // Consumer2 subscribes to customerTopic
            MessageConsumer consumer2 = session.createConsumer(dest);
            consumer2.setMessageListener(new Consumer2(connection, session, consumer2));
              
            connection.start();

        } catch (JMSException e) {
        }
    }
}
