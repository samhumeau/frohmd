package com.diffbot.frohmd.webapp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;



public class Server {
	
	public static final List<Shard> shards= new ArrayList<Shard>();

	public static void init() throws IOException{
		for (String s : PropertyCluster.getAllShardsLocation())
			shards.add(new Shard(s));
	}
	
	public static int keyToShard(byte[] key){
		int acc = 257;
		for (byte b : key)
			acc = acc*31+b;
		return Math.abs(acc) % shards.size();
	}
	
	
	public static void main(String[] args) throws LifecycleException, IOException {
		init();
		Tomcat tomcat = new Tomcat();
		tomcat.setPort(PropertyCluster.getPort());

		System.out.println("starting the server, loading in memory the static resources ...");
		Context ctx = tomcat.addContext("/", new File(".").getAbsolutePath());
		
		Tomcat.addServlet(ctx, "add", new AddCollectionServlet());
		ctx.addServletMappingDecoded("/add","add");
		
		Tomcat.addServlet(ctx, "put", new PutServlet());
		ctx.addServletMappingDecoded("/put/*","put");
		
		Tomcat.addServlet(ctx, "finalize", new FinalizeServlet());
		ctx.addServletMappingDecoded("/finalize","finalize");
		
		Tomcat.addServlet(ctx, "search", new SearchServlet());
		ctx.addServletMappingDecoded("/search","search");
		
		Tomcat.addServlet(ctx, "status", new StatusServlet());
		ctx.addServletMappingDecoded("/status","status");
		
		Tomcat.addServlet(ctx, "index", new IndexServlet());
		ctx.addServletMappingDecoded("/","index");

		
		tomcat.start();
		tomcat.getServer().await();
	}

}
