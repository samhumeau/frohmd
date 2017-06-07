package com.diffbot.frohmd;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/** Main class to build a Frohmd Map */
public class FrohmdMapBuilder implements Closeable{
	int logNbBuckets = 5; // meaning the number of buckets is 2^5=32
	int nbBuckets = (int) Math.pow(2, logNbBuckets);
	HashFunction hashFunc=Hashing.murmur3_128();
	OutputStream os_datastore;
	OutputStream[] os_index;
	long currentPositionInDatastore=0L;
	public long nbKeys=0L, sizeRecord = 0L;
	public static final Charset charset=Charset.forName("UTF-8");
	String path;
	
	// parameter for compression
	boolean compress = false;
	byte[] dictionary_compression;
	ZstdDictCompress dictCompress = null;
	List<byte[]> keysForDict = new ArrayList<>();
	List<byte[]> valuesForDict = new ArrayList<>();

	
	
	public FrohmdMapBuilder(String path) throws IOException{
		init(path, false);
	}
	
	public FrohmdMapBuilder(String path, boolean compress)  throws IOException{
		init(path,compress);
	}
	
	public void init(String path, boolean compress) throws IOException{
		this.path=path;
		os_datastore=new BufferedOutputStream(new FileOutputStream(path+".data"));
		os_index=new OutputStream[nbBuckets];
		for (int i=0; i<nbBuckets; i++)
			os_index[i]=new BufferedOutputStream(new FileOutputStream(path+".index_tmp"+i));
		this.compress = compress;
	}
	
	
	public static byte[] stringToBytes(String key){
		return key.getBytes(charset);
	}
	
	public void put(String key, String data) throws IOException{
		byte[] b_key= stringToBytes(key);
		byte[] b_data= stringToBytes(data);
		put(b_key,b_data);
	}
	
	public synchronized void put(byte[] key, byte[] data) throws IOException{
		if (compress){
			if (dictCompress != null)
				actualPut(key, data);
			else{
				keysForDict.add(key);
				valuesForDict.add(data);
				if (keysForDict.size()==100){
					buildDictionary();
				}
			}
		}
		else{
			actualPut(key, data);
		}
	}
	
	private void buildDictionary() throws IOException{
		// conatenate the 100 values given as example to build the dictionary
		int sizeDict = 0;
		for (byte[] bs : valuesForDict)
			sizeDict+=bs.length;
		dictionary_compression = new byte[sizeDict];
		int counter =0;
		for (byte[] bs : valuesForDict)
			for (int i=0; i<bs.length; i++){
				dictionary_compression[counter] = bs[i];
				counter++;
			}
		dictCompress = new ZstdDictCompress(dictionary_compression, 1);
		for (int i=0; i<keysForDict.size(); i++){
			actualPut(keysForDict.get(i), valuesForDict.get(i));
		}
	}
	
	
	private void actualPut(byte[] key, byte[] data) throws IOException{
		long hash=hashFunc.hashBytes(key).asLong();
		int bucketId= getBucketorSlotId(hash, logNbBuckets, nbBuckets);
		byte[] compressed = data;
		if (compress){
			compressed = Zstd.compress(data, dictCompress);
		}
		int lengthData=data.length;
		int lengthCompressed = compressed.length;
		os_datastore.write(compressed);
		
		byte[] il = IndexLine.toLine(hash, currentPositionInDatastore, lengthCompressed, lengthData);
		os_index[bucketId].write(il);
		currentPositionInDatastore+=lengthCompressed;
		nbKeys++;
		sizeRecord+=lengthCompressed;
	}
	
	
	
	private void closeOS(){
		if (compress && dictCompress == null){
			try {
				buildDictionary();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			os_datastore.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (int i=0; i<nbBuckets; i++){
			try {
				os_index[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	private void sortIndex(){
		int logNbSlots=Math.max(logNbBuckets,(int) (Math.log(Math.max(1,1.0*nbKeys/30))/Math.log(2)));
		int nbSlots=(int) Math.pow(2, logNbSlots);
		OutputStream io_index=null, io_headIndex=null;
		try {
			io_index=new BufferedOutputStream(new FileOutputStream(path+".indexBody"));
			io_headIndex=new BufferedOutputStream(new FileOutputStream(path+".indexHead"));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
		long currentSlotId=0L;
		long positionSlotInIndexBody=0L;
		long currentPositionInIndexBody=0L;
		int nbbytesInCurrentSlot=0;
		for (int i=0; i<nbBuckets; i++){
			File f=new File(path+".index_tmp"+i);
			try {
				InputStream is=new BufferedInputStream(new FileInputStream(f));
				List<IndexLine> allLines=new ArrayList<IndexLine>();
				byte[] buffer=new byte[IndexLine.sizeLine];
				int nbByteRead=is.read(buffer);
				while(nbByteRead!=-1){
					allLines.add(new IndexLine(buffer));
					nbByteRead=is.read(buffer);
				}
				is.close();
				Collections.sort(allLines, (l1,l2) -> Long.compare(l1.hash, l2.hash));
						
				for (IndexLine line : allLines){
					int slotId=getBucketorSlotId(line.hash, logNbSlots, nbSlots);
					while(currentSlotId<slotId){
						io_headIndex.write(IndexLine.toLine(currentSlotId, positionSlotInIndexBody, nbbytesInCurrentSlot, nbbytesInCurrentSlot));
						currentSlotId++;
						nbbytesInCurrentSlot=0;
						positionSlotInIndexBody=currentPositionInIndexBody;
					}
					io_index.write(IndexLine.toLine(line.hash, line.position, line.lengthCompressed, line.lengthUncompressed));
					nbbytesInCurrentSlot+=IndexLine.sizeLine;
					currentPositionInIndexBody+=IndexLine.sizeLine;
				}
				f.delete();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		while(currentSlotId<nbSlots){
			try {
				io_headIndex.write(IndexLine.toLine(currentSlotId, positionSlotInIndexBody, nbbytesInCurrentSlot,nbbytesInCurrentSlot));
			} catch (IOException e) {
				e.printStackTrace();
			}
			currentSlotId++;
			nbbytesInCurrentSlot=0;
			positionSlotInIndexBody=currentPositionInIndexBody;
		}
		try {
			io_headIndex.close();
			io_index.close();
			DataOutputStream dos=new DataOutputStream(new FileOutputStream(path+".mapProperties"));
			dos.writeInt(logNbSlots);
			dos.writeLong(nbSlots);
			dos.writeLong(nbKeys);
			dos.writeLong(sizeRecord);
			dos.writeBoolean(compress);
			if (compress){
				dos.writeInt(dictionary_compression.length);
				dos.write(dictionary_compression);
				System.out.println(Arrays.hashCode(dictionary_compression));
				dictCompress.close();
			}
			dos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static int getBucketorSlotId(long hash, int logNb, long nb){
		int bucketId= (int) (hash >> (64-logNb));
		return (int) (bucketId+(nb/2));
	}


	@Override
	public void close() throws IOException {
		closeOS();
		sortIndex();
		
	}
	
	public static void printIndexFile(String path){
		try{
			InputStream is=new BufferedInputStream(new FileInputStream(path));
			byte[] buffer=new byte[IndexLine.sizeLine];
			int nbByteRead=is.read(buffer);
			while(nbByteRead!=-1){
				IndexLine il=new IndexLine(buffer);
				System.out.println(il.hash+","+il.position+","+il.lengthCompressed+","+il.lengthUncompressed);
				nbByteRead=is.read(buffer);
			}
			is.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException{
		long start = System.nanoTime();
		FrohmdMapBuilder fmb=new FrohmdMapBuilder("testIndex", true);
		for (int i=0; i<1_000_000; i++){
			if (i%100000 == 0)
				System.out.println(i);
			fmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
		}
		System.out.println((System.nanoTime()-start)/1e6+"ms to insert");
		start = System.nanoTime();
		fmb.close();
		System.out.println((System.nanoTime()-start)/1e6+"ms to index");
	}

}
