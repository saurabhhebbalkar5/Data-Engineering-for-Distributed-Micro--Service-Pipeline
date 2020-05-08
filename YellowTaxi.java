package yellowcab;

public class YellowTaxi {

    public static void main(String[] args) throws Exception { 
        // load crash data 
        CrashTask.run();
        
        // initialize publisher
        thread(new Publisher(), false);
        
        // Initialize gateway
        thread(new ConsumerFactory(), false);
    }

    // Run Threads
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
}