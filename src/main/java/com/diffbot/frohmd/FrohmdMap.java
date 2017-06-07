package com.diffbot.frohmd;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictDecompress;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class FrohmdMap implements Closeable{
	
	private static final Charset charset=Charset.forName("UTF-8");
	private  RandomAccessForLargeFile accessIndexHead;
	private RandomAccessForLargeFile accessIndexBody;
	private RandomAccessForLargeFile accessData;
	private long nbSlots;
	private int logNbSlots;
	public long nbKeys, sizeData;
	private HashFunction hashFunc=Hashing.murmur3_128();
	
	// for compression
	boolean compress = false;
	private ZstdDictDecompress dictDecompress;

	public FrohmdMap(String path) throws IOException {
		accessData=new RandomAccessForLargeFile(new File(path+".data"));
		accessIndexBody=new RandomAccessForLargeFile(new File(path+".indexBody"));
		accessIndexHead=new RandomAccessForLargeFile(new File(path+".indexHead"));
		
		DataInputStream dis=new DataInputStream(new FileInputStream(path+".mapProperties"));
		logNbSlots=dis.readInt();
		nbSlots=dis.readLong();
		nbKeys= dis.readLong();
		sizeData = dis.readLong();
		compress = dis.readBoolean();
		if (compress){
			int sizeDict = dis.readInt();
			byte[] buffer = new byte[sizeDict];
			dis.read(buffer);
			dictDecompress = new ZstdDictDecompress(buffer);
		}
		dis.close();
	}
	
	public static String bytesToString(byte[] bytes){
		return new String(bytes, charset);
	}
	
	public String getString(String key){
		byte[] b_key=key.getBytes(charset);
		byte[] b_data=get(b_key);
		if (b_data==null)
			return null;
		String s = new String(b_data, charset);
		return s;
	}
	
	
	public synchronized byte[] get(byte[] key){
		long hash=hashFunc.hashBytes(key).asLong();
		long positionInIndexHead=FrohmdMapBuilder.getBucketorSlotId(hash, logNbSlots, nbSlots);
		IndexLine headLine=new IndexLine(accessIndexHead.getBytes(positionInIndexHead*IndexLine.sizeLine, IndexLine.sizeLine));
		
		byte[] slot=accessIndexBody.getBytes(headLine.position, headLine.lengthCompressed);
		//System.out.println(slot.length+"<-slot");
		IndexLine indexBodyLine=null;
		for (int i=0; i<slot.length; i+=IndexLine.sizeLine){
			byte[] buffer=Arrays.copyOfRange(slot, i, i+IndexLine.sizeLine);
			IndexLine line=new IndexLine(buffer);
			if (line.hash==hash){
				indexBodyLine=line;
				break;
			}
		}
		if (indexBodyLine==null)
			return null;
		
		byte[] dataCompressed=accessData.getBytes(indexBodyLine.position, indexBodyLine.lengthCompressed);
		byte[] dataUncompressed = dataCompressed;
		if (compress){
			dataUncompressed = Zstd.decompress(dataCompressed, dictDecompress, indexBodyLine.lengthUncompressed);
		}
		return dataUncompressed;
	}
	
	
	@Override
	public void close() throws IOException  {
		accessData.close();
		accessIndexBody.close();
		accessIndexHead.close();
	}
	
	public static void main(String[] args) throws IOException {
		FrohmdMap map=new FrohmdMap("testIndex");
		System.out.println(map.getString("key"+1));
//		long start=System.nanoTime();
//		Random rand=new Random();
//		int nbError=0;
//		for (int i=0; i<10000; i++){
//			int ri=rand.nextInt(5_000_000);
//			String s=map.getString("key"+ri);
//			if (!s.endsWith(String.valueOf(ri)))
//					nbError++;
//		}
//		
//		System.out.println("10,000 random reads in "+(System.nanoTime()-start)/1e6+"ms");
//		System.out.println(nbError);
		map.close();
	}
	
	
}
