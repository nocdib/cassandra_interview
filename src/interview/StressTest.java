package interview;

import java.io.*;

import com.datastax.driver.core.Session;

public class StressTest implements Runnable {
	Session session;
	Operation testType;
	int num_queries;
	static final int NUM_QUERIES = 50000;

	public StressTest(Session session){
		this(session, Operation.MIXED, NUM_QUERIES);
	}

	public StressTest(Session session, Operation action, int num_queries){
		this.session = session;
		this.testType = action;
		this.num_queries = num_queries;
	}

	public void run() {


		System.out.printf("Starting %s stress test with %d queries\n\n", testType, num_queries);
		long startTime = System.nanoTime();

		try {
			String output = "";
        	String stressToolPath = "/Users/nocdib/Development/dsc-cassandra-2.2.0/tools/bin/cassandra-stress";
            Process p = Runtime.getRuntime().exec(new String[]{stressToolPath,testType.toString().toLowerCase(),"n=" + num_queries});

            BufferedReader stdInput = new BufferedReader(new
                 InputStreamReader(p.getInputStream()));

            // read the output from the command
            System.out.println("Here is the standard output of the command:\n");
            while ((output = stdInput.readLine()) != null) {
                //System.out.println(output);
            }

            double estimatedTime = (System.nanoTime() - startTime)/1_000_000_000f;
    		System.out.printf("%s stress test finished in: ", testType);
    		if (estimatedTime > 59.9f)
    			System.out.printf("%d minute(s) %.2f seconds\n", (int)estimatedTime/60, estimatedTime % 60f);
    		else
    			System.out.printf("%.2f seconds\n", estimatedTime);
        }
        catch (IOException e) {
            System.out.println("exception..");
            e.printStackTrace();
            System.exit(-1);
        }
    }


}
