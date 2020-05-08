package yellowcab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import java.util.stream.Stream;
import static yellowcab.CrashTask.getCrashCountOnBusiestLoc;
import static yellowcab.Consumer2.locationID;

public class BusiestLocation implements Runnable {

    String key;
    String fileName = "taxi+_zone_lookup.csv";

    // Set Key to the input key
    public BusiestLocation(String key) {
        this.key = key;
    }

    @Override
    public void run() {
        try {

            // Collection to store all the location ID for secified key
            Collection<Integer> busiestLoc = locationID.get(key);

            // Count the frequency of trips started at Pickup location
            Map<Integer, Long> collect = busiestLoc.stream().collect(groupingBy(Function.identity(), counting()));
            
            // get location ID where maximum trip booking are initiated
            int maxLoc = collect.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).get().getKey();

            // Borough and Zone Lookup for the identified busy location ID
            try ( Stream<String> data = Files.lines(Paths.get(fileName)).filter(s -> !s.isEmpty())) {
                data.skip(1).map(s -> new AbstractMap.SimpleEntry<>(s, s.split(",")))
                        .filter(s -> s.getValue()[0].equals(String.valueOf(maxLoc)))
                        .forEach(s -> {
                            System.out.println("Buisest Location : " + s.getValue()[1] + "," + s.getValue()[2]);
                            try {
                                // Get Accident count on the busiest Location
                                getCrashCountOnBusiestLoc(Integer.parseInt(key.split(",")[0]),
                                        Integer.parseInt(key.split(",")[1]), s.getValue()[1].replaceAll("^\"|\"$", ""));
                            } catch (IOException ex) {
                            }
                        });
            }
        } catch (IOException e) {
        }

    }
}
