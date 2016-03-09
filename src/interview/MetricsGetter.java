package interview;

import javax.management.remote.JMXServiceURL;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import com.datastax.driver.core.Session;

public class MetricsGetter implements Runnable{
	Session session;
	String host;
	int port;
	boolean shouldRunMetrics;
	int count;

	public MetricsGetter(Session session, String host, int port){
		this.host = host;
		this.port = port;
		this.session = session;
		shouldRunMetrics = true;
		count = 1;
	}

	public void run() {
		try{
				JMXServiceURL target = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
				JMXConnector connector = JMXConnectorFactory.connect(target);
				MBeanServerConnection remote = connector.getMBeanServerConnection();
				MBeanInfo info;
				MBeanAttributeInfo[] attributes;

    			String AllMemtablesHeapSize = "org.apache.cassandra.metrics:type=ColumnFamily,name=AllMemtablesHeapSize";
				ObjectName AllMemtablesHeapSizeBean = new ObjectName(AllMemtablesHeapSize);

				String AllMemtablesLiveDataSize = "org.apache.cassandra.metrics:type=ColumnFamily,name=AllMemtablesLiveDataSize";
				ObjectName AllMemtablesLiveDataSizeBean = new ObjectName(AllMemtablesLiveDataSize);

				String LiveSSTableCount = "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=keyspace1,scope=standard1,name=LiveSSTableCount";
				//"org.apache.cassandra.metrics:type=ColumnFamily,name=LiveSSTableCount";
				ObjectName LiveSSTableCountBean = new ObjectName(LiveSSTableCount);

				String x95thPercentile = "org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency";//"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency";
				//"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency";
				ObjectName x95thPercentileBean = new ObjectName(x95thPercentile);

				//clear the database metrics tables
				session.execute("truncate AllMemtablesHeapSize");
				session.execute("truncate AllMemtablesLiveDataSize");
				session.execute("truncate LiveSSTableCount");
				session.execute("truncate x95thPercentile");

				do{
					info = remote.getMBeanInfo(AllMemtablesHeapSizeBean);
					attributes = info.getAttributes();
					long AllMemtablesHeapSizeValue = (long) remote.getAttribute(AllMemtablesHeapSizeBean,attributes[0].getName());
					System.out.println("AllMemtablesHeapSize - " + AllMemtablesHeapSizeValue);
					session.execute(	"insert into AllMemtablesHeapSize(id, value) values(" +
										count + "," + AllMemtablesHeapSizeValue + ")");

					info = remote.getMBeanInfo(AllMemtablesLiveDataSizeBean);
					attributes = info.getAttributes();
					long AllMemtablesLiveDataSizeValue = (long) remote.getAttribute(AllMemtablesLiveDataSizeBean,attributes[0].getName());
					System.out.println("AllMemtablesLiveDataSize - " + AllMemtablesLiveDataSizeValue);
					session.execute(	"insert into AllMemtablesLiveDataSize(id, value) values(" +
										count + "," + AllMemtablesLiveDataSizeValue + ")");

					info = remote.getMBeanInfo(LiveSSTableCountBean);
					attributes = info.getAttributes();
					int LiveSSTableCountValue = (int) remote.getAttribute(LiveSSTableCountBean,attributes[0].getName());
					System.out.println("LiveSSTableCount - " + LiveSSTableCountValue);
					session.execute(	"insert into LiveSSTableCount(id, value) values(" +
										count + "," + LiveSSTableCountValue + ")");

					info = remote.getMBeanInfo(x95thPercentileBean);
					attributes = info.getAttributes();
					/*
					for(MBeanAttributeInfo attribute : attributes)
						System.out.println(attribute.getName());

					System.out.println(attributes.length);*/
					double x95thPercentileValue = (double) remote.getAttribute(x95thPercentileBean,"95thPercentile");
					System.out.println("95thPercentile - " + x95thPercentileValue);
					session.execute(	"insert into x95thPercentile(id, value) values(" +
										count + "," + x95thPercentileValue + ")");

					System.out.println("--------------------------");

					Thread.sleep(1000);
					count++;
				}while(shouldRunMetrics);
			}catch(Exception e){
				System.out.println("Exception in MetricsGetter\n" + e);
				e.printStackTrace();
		    }
	}//main

	public void setRunner(boolean bool){
		shouldRunMetrics = bool;
	}
}
