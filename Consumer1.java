package yellowcab;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import static yellowcab.YellowTaxi.thread;

public class Consumer1 implements MessageListener {

    static String key;

    // Initialize 1D array to hold counter for each record received 
    static int[] dateTime = new int[24];

    @Override
    public void onMessage(Message message) {
        try {
            int dayFreq = 0, hourFreq = 0;

            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;

                /*
                    When the message is not reached eof.
                        1. Enable Hourly Tumbling Window
                        2. Enable Daily Tumbling Window
                 */
                if (!(textMessage.getText().equals("eof"))) {

                    // Split the message into date and time
                    // Also split the date into dd, mm and yyyy to track the counter and array writing
                    // split the time to hh mm and ss
                    String day = textMessage.getText().split(",")[0].split(" ")[0];
                    int dd = Integer.parseInt(day.split("-")[2]);
                    int mm = Integer.parseInt(day.split("-")[1]);
                    int yyyy = Integer.parseInt(day.split("-")[0]);
                    String time = textMessage.getText().split(",")[0].split(" ")[1];
                    int hh = Integer.parseInt(time.split(":")[0]);

                    // Hourly Tumbling window
                    if (hh == 0 && dateTime[hh] == 0) {
                        dateTime[hh] += 1;
                    } else if (hh != 0 && dateTime[hh] == 0) {
                        dateTime[hh] += 1;
                        hourFreq = dateTime[hh - 1];
                        key = dd + "," + (hh - 1);
                        System.out.println("Total Trips between " + (hh - 1) + " to " + hh + " : " + hourFreq);
                        thread(new BusiestLocation(key), false);
                    } 
                    
                    // Daily Tumbling window
                    else if (hh == 0 && dateTime[23] != 0) {
                        hourFreq = dateTime[23];
                        key = (dd - 1) + "," + 23;
                        System.out.println("Total Trips between " + 23 + " to " + 24 + " : " + hourFreq);
                        // Identify and print busiest location
                        thread(new BusiestLocation(key), false);

                        // Summarise whole day's trip frequency
                        for (int i = 0; i < 24; i++) {
                            dayFreq += dateTime[i];
                        }
                        Thread.sleep(500);
                        System.out.println("Total Trips made on " + (dd - 1) + "-" + mm + "-" + yyyy + " : " + dayFreq);
                        // Identify and print peak time
                        thread(new PeakTime(), false);
                        Thread.sleep(500);
                        dateTime = new int[24];
                        dateTime[hh] += 1;
                    } else {
                        dateTime[hh] += 1;
                    }
                }
                /*
                    If the stream reaches the eof, print:
                        1. 23rd to 24th hourly statistics 
                        2. Last day's peak time and total trip frequency
                    This data is hardcoded as we need to process only for January 2018
                */
                else if (textMessage.getText().equals("eof")) {

                    hourFreq = dateTime[23];
                    key = 31 + "," + 23;
                    System.out.println("Total Trips between " + 23 + " to " + 24 + " : " + hourFreq);
                    // Identify and print busiest location
                    thread(new BusiestLocation(key), false);

                    // Summarise whole day's trip frequency
                    for (int i = 0; i < 24; i++) {
                        dayFreq += dateTime[i];
                    }
                    Thread.sleep(500);
                    System.out.println("Total Trips made on " + 31 + "-" + 01 + "-" + 2018 + " : " + dayFreq);
                    // Identify and print peak time
                    thread(new PeakTime(), false);
                }
            }
        } catch (NumberFormatException | JMSException e) {
        } catch (InterruptedException ex) {
            Logger.getLogger(Consumer1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
