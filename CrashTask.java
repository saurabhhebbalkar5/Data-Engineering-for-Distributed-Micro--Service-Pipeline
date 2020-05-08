package yellowcab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CrashTask {

    // Data Strcuture to store Crash data
    static List<String> crashData = new ArrayList<>();
    static String fileName2 = "Motor_Vehicle_Collisions_-_Crashes.csv";
    static long count;

    public static void run() {
        // Stream Crash.csv
        try ( Stream<String> data = Files.lines(Paths.get(fileName2)).filter(s -> !s.isEmpty())) {
            data.map(s -> new AbstractMap.SimpleEntry<>(s, s.split(",")))
                    // Filter Jan 2018 from the csv
                    .filter(s -> (s.getValue()[0].split("/")[0].equals("01")
                    && s.getValue()[0].split("/")[2].equals("2018")))
                    .forEach(s -> {
                        // Handle empty values in the Borough field
                        if (s.getValue()[2].isEmpty()) {
                            s.getValue()[2] = "Location Not Available";
                        }
                        String crash = s.getValue()[0] + "," + s.getValue()[1] + "," + s.getValue()[2];
                        // Add Date, Time and Borough deatils to the list
                        crashData.add(crash);

                    });
        } catch (IOException ex) {
        }
    }

    public static void getCrashCountOnBusiestLoc(int date, int time, String borough) throws IOException {
        // List to store crash details of specified date time
        List<String> temp = new ArrayList<>();
        crashData.parallelStream()
                .map(s -> new AbstractMap.SimpleEntry<>(s, s.split(",")))
                // Filter crashData on specified date time
                .filter(s -> Integer.parseInt(s.getValue()[0].split("/")[1]) == date
                && Integer.parseInt(s.getValue()[1].split(":")[0]) == time)
                .forEach(s -> {
                    String crash = s.getValue()[0] + "," + s.getValue()[1] + "," + s.getValue()[2];
                    // Add crash details of specified date time to a temperory list
                    temp.add(crash);
                });

        // Count the number of accidents occured in specified bororugh
        count = temp.parallelStream()
                .map(s -> new AbstractMap.SimpleEntry<>(s, s.split(",")))
                .filter(s -> s.getValue()[2].toLowerCase().contains(borough.toLowerCase()))
                .count();
        System.out.println("Number of accidents at " + borough + " : " + count);
        
        // Count the number of accidents occured overall city
        count = temp.parallelStream().count();
        System.out.println("Number of accidents across city : " + count);

        // Get the Location details of the accidents occured overall city
        List<String> t = new ArrayList<>();
        temp.stream()
                .map(s -> new AbstractMap.SimpleEntry<>(s, s.split(",")))
                .forEach(s -> t.add(s.getValue()[2]));

        Set<String> distinct = new HashSet<>(t);
        // Print the frequency in each location
        distinct.forEach((s) -> {
            System.out.println(s.toUpperCase() + ": " + Collections.frequency(t, s));
        });
        System.out.println();
    }
}
