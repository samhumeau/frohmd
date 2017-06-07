package com.diffbot.frohmd.webapp;

import java.util.Arrays;
import java.util.List;

public class PropertyCluster {
	
	public static List<String> getAllShardsLocation(){
		return Arrays.asList("./data/shard1", "./data/shard2", "./data/shard3", "./data/shard4");
	}

}
