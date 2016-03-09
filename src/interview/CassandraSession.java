package interview;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.*;

public class CassandraSession {
	private String sContactPoint;
	private String sKeyspace;
	private Cluster cluster = null;
	private Session session = null;

	public CassandraSession(){
		this("127.0.0.1", "keyspace1");
	}

	public CassandraSession(String sContactPoint, String sKeyspace){
		this.sContactPoint = sContactPoint;
		this.sKeyspace = sKeyspace;

		try{
			cluster = Cluster.builder().addContactPoint(sContactPoint).build();
			session = cluster.connect(sKeyspace);
			System.out.printf("Connected to the keyspace \"%s\" on %s\n", sKeyspace, sContactPoint);
		}catch(NoHostAvailableException e){
			System.out.println("Cassandra must be installed and running for this test to work.\n"
					+ "Make sure that the address to your cluster is valid.");
			System.exit(-1);
		}catch(InvalidQueryException e){
			System.out.println("The keyspace you are trying to connect to does not exist");
			System.exit(-2);
		}
	}

	public Session getSession(){
		return this.session;
	}



} //class
