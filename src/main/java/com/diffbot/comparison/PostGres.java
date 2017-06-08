package com.diffbot.comparison;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/** Check the speed of injection using PostGres */
public class PostGres {
	
	public static void main(String[] args) {
		Connection c = null;
		Statement stmt = null;
	      try {
	    	 long start = System.nanoTime();
	         Class.forName("org.postgresql.Driver");
	         c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/sam","sam", "sam");
	         c.setAutoCommit(false);
	         stmt = c.createStatement();
	         String sql = "CREATE TABLE IF NOT EXISTS tdata (key TEXT, value TEXT)";
	         stmt.executeUpdate(sql);
	         stmt.close();
	         c.commit();
	         int counter =0;
	         for (int j=0; j<100; j++){
		         CopyManager copyManager = new CopyManager((BaseConnection) c);
		         CopyIn copyIn = copyManager.copyIn("COPY TDATA FROM STDIN WITH DELIMITER '|'");
		         for (int i=0; i<1_000_000; i++){
		        	 counter++;
		        	 if (counter%100000 == 0)
		        		 System.out.println(counter);
			         String st = "key"+counter+"|"+"This is the value (and it is quite a very very very long value) for the key. "+counter+"\n";
			         byte[] bytes = st.getBytes(StandardCharsets.UTF_8);
			         copyIn.writeToCopy(bytes, 0, bytes.length);
		 		}
		         copyIn.endCopy();
		         c.commit();
	         }
	         System.out.println((System.nanoTime()-start)/1e6+"ms to insert");
	         start = System.nanoTime();
	         stmt = c.createStatement();
	         sql = "CREATE INDEX idx ON tdata (key);";
	         stmt.executeUpdate(sql);
	         stmt.close();
	         c.commit();
	         System.out.println((System.nanoTime()-start)/1e6+"ms to index");
	         start = System.nanoTime();
	         Random r = new Random();
	         for (int i=0; i<10000; i++){
		         stmt = c.createStatement();
		         ResultSet rs = stmt.executeQuery( "select value from tdata where tdata.key = 'key"+r.nextInt(5000000)+"';" );
		         rs.close();
	         }
	         System.out.println((System.nanoTime()-start)/1e6+"ms to query 10,000");
	         c.close();
	      } catch (Exception e) {
	         e.printStackTrace();
	         System.err.println(e.getClass().getName()+": "+e.getMessage());
	         System.exit(0);
	      }
	      System.out.println("Opened database successfully");
	}

}
