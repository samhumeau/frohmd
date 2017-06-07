package com.diffbot.frohmd;

import java.util.Arrays;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class IndexLine{
	long hash;
	public long position;
	public int lengthCompressed;
	public int lengthUncompressed;
	public static final int sizeLine = 24;
	
	public IndexLine(byte[] line) {
		hash=Longs.fromByteArray(Arrays.copyOfRange(line, 0, 8));
		position=Longs.fromByteArray(Arrays.copyOfRange(line, 8, 16));
		lengthCompressed=Ints.fromByteArray(Arrays.copyOfRange(line, 16, 20));
		lengthUncompressed=Ints.fromByteArray(Arrays.copyOfRange(line, 20, 24));
	}
	public static byte[] toLine(long l1, long l2, int lengthCompressed, int lengthUncompressed){
		byte[] line=new byte[24];
		byte[] l1_b=Longs.toByteArray(l1);
		byte[] l2_b=Longs.toByteArray(l2);
		byte[] i_b=Ints.toByteArray(lengthCompressed);
		byte[] iu_b=Ints.toByteArray(lengthUncompressed);
		for (int j=0; j<8; j++)
			line[j]=l1_b[j];
		for (int j=0; j<8; j++)
			line[j+8]=l2_b[j];
		for (int j=0; j<4; j++)
			line[j+16]=i_b[j];
		for (int j=0; j<4; j++)
			line[j+20]=iu_b[j];
		return line;
	}
	@Override
	public String toString() {
		return hash+","+position+","+lengthCompressed+","+lengthUncompressed;
	}
}
