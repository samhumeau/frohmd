package com.diffbot.comparison;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class Cassandra {
	
	public static void main(String[] args) {
		Cluster cluster = null;
		try {
		    cluster = Cluster.builder()                                                    
		            .addContactPoint("127.0.0.1")
		            .build();
		    Session session = cluster.connect();   
		    Statement s = new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS tp2 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		    session.execute(s);
		    session.close();
		    session = cluster.connect("tp2");   
		    s = new SimpleStatement("DROP TABLE IF EXISTS test;");
		    session.execute(s);
		    s = new SimpleStatement("CREATE TABLE IF NOT EXISTS test (key text, value text);");
		    session.execute(s);
		    PreparedStatement prepared = session.prepare("insert into test (key, value) values (?, ?);");
		    for (int i=0; i<20000000; i++){
		    	if (i%100000 == 0)
		    		System.out.println(i);
		    	session.executeAsync(prepared.bind("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i));
		    		
		    }
		} finally {
		    if (cluster != null) cluster.close();  
		}
	}

}
