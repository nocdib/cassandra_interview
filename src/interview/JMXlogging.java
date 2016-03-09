package interview;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.*;
import com.example.ThreadTask;
import com.journaldev.jmx.ThreadTest;

import org.apache.cassandra.metrics.ColumnFamilyMetrics; // AllMemTablesDataSize
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.tools.NodeTool;


public class JMXlogging {

	static String host = "127.0.0.1";
	static int port = 7199;
	static String keyspace = "demo";
	static String outputFile = "JMXmetrics.html";

	public static void main(String[] args) {

		int num_queries = 1000000;
		CassandraSession oCassandraSession = new CassandraSession(host,keyspace);
		Session session = oCassandraSession.getSession();

		StressTest oStressTest =  new StressTest(session, Operation.WRITE, num_queries);
		MetricsGetter oMetricsGetter = new MetricsGetter(session, host, port);

		Thread stressTestThread = new Thread(oStressTest);
		Thread metricsGetterThread = new Thread(oMetricsGetter);

		metricsGetterThread.start(); //gather metrics
		stressTestThread.start();	//run stress test

		while(stressTestThread.isAlive()){	// wait until stress test is finished
		}
		oMetricsGetter.setRunner(false);	//stop the metrics getter

		// plot the points
		String [] outputMetrics = {"AllMemtablesHeapSize","AllMemtablesLiveDataSize",
									"LiveSSTableCount","x95thPercentile"};
		OutputResults output = new OutputResults(session, outputMetrics, outputFile);

		System.exit(0);

	}// main()

}
