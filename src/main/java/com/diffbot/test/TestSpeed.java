package com.diffbot.test;
import java.util.Random;


public class TestSpeed {
	public static void main(String[] args) {
	//	HashFunction h=Hashing.murmur3_128();
		Random rand=new Random();
		long start=System.nanoTime();
		int nbErrors=0;
		for (int i=0; i<1000000; i++){
			byte[] input=new byte[10];
			rand.nextBytes(input);
////			byte[] output=h.hashBytes(input).asLong()
//			if (output.length>9)
//				nbErrors++;
			
		}
		long end=System.nanoTime();
		System.out.println(end-start);
		System.out.println(nbErrors);
	}
}
