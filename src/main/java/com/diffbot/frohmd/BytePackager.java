package com.diffbot.frohmd;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Ints;

public class BytePackager {
	
	/** package a array of byte[] to a single byte[], tries to be efficient in 
	 *  the size of byte returned.
	 * @return
	 */
	public static byte[] pack(byte[]... arrays){
		int sizePack=4;
		for (byte[] b : arrays){
			sizePack+=(4+b.length);
		}
		byte[] pack_ = new byte[sizePack];
		copyOn(pack_, Ints.toByteArray(arrays.length),0);
		int state=4;
		for (byte[] b : arrays){
			copyOn(pack_, Ints.toByteArray(b.length),state);
			state+=4;
			copyOn(pack_, b, state);
			state+=b.length;
		}
		return pack_;
	}
	
	public static byte[] pack(List<byte[]> arrays){
		byte[][] tmp=new byte[arrays.size()][];
		for (int i=0; i<arrays.size(); i++)
			tmp[i]=arrays.get(i);
		return pack(tmp);
	}
	
	public static byte[][] unpack(byte[] pack){
		int nbPacks=Ints.fromByteArray(Arrays.copyOfRange(pack, 0, 4));
		byte[][] depack=new byte[nbPacks][];
		int state=4;
		for (int i=0; i<nbPacks; i++){
			int l=Ints.fromByteArray(Arrays.copyOfRange(pack, state, state+4));
			state+=4;
			depack[i]=Arrays.copyOfRange(pack, state, state+l);
			state+=l;
		}
		return depack;
	}
	
	public static void copyOn(byte[] out, byte[] pattern, int offset){
		for (int i=0; i<pattern.length; i++){
			out[offset+i]=pattern[i];
		}
	}
	
	public static void main(String[] args) {
		byte[] b1={1,2,3,4,5,6};
		byte[] b2={5,6,7,8,9};
		byte[] b3={10,11,12,13,14};
		
		byte[] pack_=pack(b1,b2,b3);
		System.out.println(Arrays.toString(pack_));
		byte[][] unpacked=unpack(pack_);
		for (int i=0; i<unpacked.length; i++)
			System.out.println(Arrays.toString(unpacked[i]));
	}
	

}
