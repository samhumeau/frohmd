package com.diffbot.frohmd;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;

public class FrohmdIndexBuilder implements Closeable{
	String path;
	int logNbBuckets = 8; // meaning the number of buckets is 2^5=32
	int nbBuckets = (int) Math.pow(2, logNbBuckets);
	HashFunction hashFunc=Hashing.murmur3_128();
	OutputStream[] os_tmp;
	public static final Charset charset=Charset.forName("UTF-8");
	boolean isClosed=false;
	
	public FrohmdIndexBuilder(String path) {
		this.path=path;
		try{
			os_tmp=new OutputStream[nbBuckets];
			for (int i=0; i<nbBuckets; i++)
				os_tmp[i]=new BufferedOutputStream(new FileOutputStream(path+".dataIndex_tmp"+i));
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	public synchronized void add(String key, String value){
		if (isClosed)
			return;
		byte[] b_key=key.getBytes(charset);
		byte[] b_data=value.getBytes(charset);
		int bucketId= Math.abs(key.hashCode()) % nbBuckets;
		try {
			os_tmp[bucketId].write(Ints.toByteArray(b_key.length));
			os_tmp[bucketId].write(b_key);
			os_tmp[bucketId].write(Ints.toByteArray(b_data.length));
			os_tmp[bucketId].write(b_data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		isClosed=true;
		for (int i=0; i<nbBuckets; i++){
			try {
				os_tmp[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try(FrohmdMapBuilder fmb=new FrohmdMapBuilder(path)){
			ExecutorService es=Executors.newFixedThreadPool(4);
			for (int i=0; i<nbBuckets; i++){
				final int final_i=i;
				es.execute(new Runnable() {
					@Override
					public void run() {
						try{
							Map<String,List<byte[]>> allPairs=new HashMap<String, List<byte[]>>(1000000);
							File f_tmp=new File(path+".dataIndex_tmp"+final_i);
							InputStream is=new BufferedInputStream(new FileInputStream(f_tmp));
							while(true){
								byte[] buf1=new byte[4];
								int nbRead=is.read(buf1);
								if (nbRead<4)
									break;
								int length=Ints.fromByteArray(buf1);
								buf1=new byte[length];
								is.read(buf1);
								String stringEquivalent=new String(buf1, charset);
								
								byte[] buf2=new byte[4];
								is.read(buf2);
								length=Ints.fromByteArray(buf2);
								buf2=new byte[length];
								is.read(buf2);
								
								List<byte[]> list=allPairs.get(stringEquivalent);
								if (list==null){
									list=new ArrayList<byte[]>();
									allPairs.put(stringEquivalent, list);
								}
								list.add(buf2);
							}
							is.close();
	
							List<PairByteArray> keyValues=new ArrayList<PairByteArray>(allPairs.size());
							for (String key : allPairs.keySet()){
								byte[] back=key.getBytes(charset);
								keyValues.add(new PairByteArray(back, BytePackager.pack(allPairs.get(key))));
							}
							synchronized (fmb) {
								for (PairByteArray pba : keyValues)
									fmb.put(pba.first, pba.second);
							}
							
							f_tmp.delete();
						}catch(Exception e){
							e.printStackTrace();
						}
					}
				});
			}
			es.shutdown();
			while(!es.isTerminated()){
				es.awaitTermination(1, TimeUnit.MILLISECONDS);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	
	
	
	

}
