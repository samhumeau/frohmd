package com.diffbot.test;

import java.util.Arrays;
import java.util.Random;

public class TestPosition {
	
	public static final int nbItems=10_000_000;
	
	public static void testDistance(){
		Random rand=new Random();
		long[] longs=new long[nbItems];
		for (int i=0; i<nbItems; i++){
			longs[i]=rand.nextLong();
		}
		Arrays.sort(longs);
		
		long[] sampe=new long[100];
		for (int i=0; i<sampe.length; i++){
			sampe[i]=longs[i+4999950];
		}
		System.out.println(Arrays.toString(sampe));
		System.out.println(getApproxPosition(longs[5000000]));
		
		
//		List<Integer> distances=new ArrayList<Integer>();
//		for(int i=0; i<8000; i++){
//			int pos=rand.nextInt(nbItems);
//			int approxPosition=getApproxPosition(longs[pos]);
//			distances.add(pos-approxPosition);
//		}
//		
//		System.out.println(distances);
	}
	
	public static int getApproxPosition(long l){
		double d_l = (double) l;
		double half = Math.pow(2, 64);
		double prop=d_l/half;
		prop+=0.5;
		return (int) (prop*nbItems);
	}
	
	public static void main(String[] args) {
		testDistance();
	}
}
