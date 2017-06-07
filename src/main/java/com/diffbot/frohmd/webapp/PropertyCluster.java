package com.diffbot.frohmd.webapp;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.moandjiezana.toml.Toml;

public class PropertyCluster {
	static Toml toml;
	static{
		try{
			toml = new Toml().read(new File("./conf/frohmd.toml"));
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static List<String> getAllShardsLocation(){
		if (toml == null)
			return Arrays.asList("./data");
		return toml.getList("server.shards");
	}
	
	public static int getPort(){
		if (toml == null)
			return 6800;
		return toml.getLong("server.port").intValue();
	}

}
