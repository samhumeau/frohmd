package com.diffbot.frohmd.webapp;

import com.diffbot.frohmd.RemoteFrohmdMapBuilder;

public class InjectTest {
	
	/** Injection test */
	public static void main(String[] args) throws Exception {
		if (args.length<1){
			System.out.println("No argument provided");
			return;
		}
		long start = System.nanoTime();
		try(RemoteFrohmdMapBuilder rfmb = new RemoteFrohmdMapBuilder(args[0], "test", true);){
			for (int i=0; i<100_000_000; i++){
				if (i%100000 == 0)
					System.out.println(i+" elements injected");
				rfmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
			}
		}
		long end = System.nanoTime();
		System.out.println(end-start+"ns");
	}
}
