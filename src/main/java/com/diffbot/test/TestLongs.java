package com.diffbot.test;

import java.util.Random;

public class TestLongs {
	public static void main(String[] args) {
		Random rand=new Random();
		long l1=rand.nextLong();
		System.out.println(l1);
		int i1=(int) (l1 >> (64-5));
		System.out.println(i1);
		long l2=rand.nextLong();
		System.out.println(l2);
		int i2=(int) (l2 >> (64-5));
		System.out.println(i2);
		
	}
}
