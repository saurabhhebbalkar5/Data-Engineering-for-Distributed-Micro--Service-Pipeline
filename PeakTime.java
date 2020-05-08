package yellowcab;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Thread to calculate peak time
public class PeakTime implements Runnable {

    // Define start and end time of a day
    int startTime = 00;
    int endTime = 23;
    
    // List to hold frequency of each hour
    List<Integer> tripFreq = new ArrayList<>();
    
    // List to identify the time frame
    List<Integer> timeFrame = new ArrayList<>();

    @Override
    public void run() {
        
        // for each time, add the frequency to the list
        for (int i = startTime; i <= endTime; i++) {
            timeFrame.add(i);
            // fetch frequency from Consumer 1
            tripFreq.add(Consumer1.dateTime[i]);
        }
        
        // Calculate max frequency
        int maxVal = Collections.max(tripFreq);
        
        // Get the index of the max frequency
        // This gives the time frame of peak hour
        int maxIdx = tripFreq.indexOf(maxVal);
        System.out.println("Peak time : " + maxIdx + " to " + (maxIdx + 1));
        System.out.println("----------------------------------------------------\n");
        tripFreq.clear();
        timeFrame.clear();
    }
}
