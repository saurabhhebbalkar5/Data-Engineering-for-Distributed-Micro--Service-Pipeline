package yellowcab;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Consumer2 implements MessageListener {

    // Multimap to store all the Pickup Location at specified date time
    static Multimap<String, Integer> locationID = ArrayListMultimap.create();

    Connection connection = null;
    Session session = null;
    MessageConsumer consumer2 = null;

    // Constrcutor to set Connetion and session details 
    public Consumer2(Connection connection, Session session, MessageConsumer consumer2) {

        this.connection = connection;
        this.session = session;
        this.consumer2 = consumer2;
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                
                // If eof receieved, end the consumer, session and connection
                if (textMessage.getText().equals("eof")) {
                    consumer2.close();
                    session.close();
                    connection.close();

                } else {

                    // Get Date Time and Pickup Location ID from the Message
                    int day = Integer.parseInt(textMessage.getText().split(",")[0].split(" ")[0].split("-")[2]);
                    int time = Integer.parseInt(textMessage.getText().split(",")[0].split(" ")[1].split(":")[0]);
                    int locID = Integer.parseInt(textMessage.getText().split(",")[1]);

                    // Generate Key as Date,Time
                    String key = day + "," + time;

                    // Add multiple Pickup location IDs to date,time key
                    locationID.put(key, locID);
                }
            }
        } catch (NumberFormatException | JMSException e) {
        }
    }
}
