package praadis.util;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import static java.lang.System.out;

public class CassandraConnector {

	public static void main(final String[] args) {
		getSession();
	}

	public static Session getSession() {
		final String ipAddress = "localhost";
		final int port = 9042;
		Cluster cluster = null;
		Session session = null;
		cluster = Cluster.builder().addContactPoint(ipAddress).withPort(port).build();
		final Metadata metadata = cluster.getMetadata();
		out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
		return session;
	}
}
