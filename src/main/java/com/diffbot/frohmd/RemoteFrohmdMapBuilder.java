package com.diffbot.frohmd;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;

import com.diffbot.frohmd.webapp.PropertyCluster;
import com.diffbot.frohmd.webapp.Transmitter;
import com.google.common.primitives.Ints;


public class RemoteFrohmdMapBuilder implements Closeable{
	String serverAddress, nameCollection;
	boolean compressionForStorage;
	
	public static final int limitBytesTransmition = 5_000_000; // 5MB uncompressed
	AtomicInteger currentNbBytes = new AtomicInteger(0);
	public static final int limitNbValuesTransmittion = 200_000; // max 200_000 values transmitted
	AtomicInteger currentNbValues = new AtomicInteger(0);
	public static final int nbConcurrentTransmitters = PropertyCluster.getNbTransmitters();
	
	ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue<>();
	Transmitter[] transmitters;

	public RemoteFrohmdMapBuilder(String serverAddress, String nameCollection, boolean compressionForStorage) throws IOException{
		this.serverAddress = serverAddress;
		this.nameCollection = nameCollection;
		this.compressionForStorage = compressionForStorage;
		
		JSONObject opening = null;
		opening = Transmitter.getJSON(serverAddress+"/add?name="+nameCollection+"&compression="+compressionForStorage);
		if (opening.getBoolean("success") == false){
			throw new IOException("Exception while opening the connection to remote Frohmd server. Server says:" +opening.getString("message"));
		}
		transmitters = new Transmitter[nbConcurrentTransmitters];
		for (int i=0; i<nbConcurrentTransmitters; i++){
			transmitters[i] = new Transmitter(serverAddress, nameCollection);
			transmitters[i].start();
		}
	}
	

	public void put(String key, String value) throws Exception{
		byte[] key_b = FrohmdMapBuilder.stringToBytes(key);
		byte[] length_key = Ints.toByteArray(key_b.length);
		byte[] value_b = FrohmdMapBuilder.stringToBytes(value);
		byte[] length_value = Ints.toByteArray(value_b.length);
		byte[] concatenation = Transmitter.concatenate(length_key, key_b, length_value, value_b);
		addToTheQueue(concatenation);
	}
	
	
	private void addToTheQueue(byte[] bytes) throws IOException{
		if (currentNbBytes.get() > limitBytesTransmition || currentNbValues.get() > limitNbValuesTransmittion){
			synchronized (this) {
				if (currentNbBytes.get() > limitBytesTransmition || currentNbValues.get() > limitNbValuesTransmittion){
					flush();
				}
			}
		}
		else{
			queue.add(bytes);
			currentNbBytes.addAndGet(bytes.length);
			currentNbValues.incrementAndGet();
		}
		
	}
	
	public void flush() throws IOException{
		boolean notFoundATransmitter = true;
		while(notFoundATransmitter){
			for (Transmitter trans : transmitters){
				if (trans.encounteredProblem())
					throw new IOException("Transmitio encountered problem");
				if (trans.isAvailable()){
					trans.transmitIfAvailable(queue);
					queue = new ConcurrentLinkedQueue<>();
					currentNbBytes.set(0);
					currentNbValues.set(0);
					notFoundATransmitter = false;
					break;
				}
			}
			if (notFoundATransmitter){
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		try{
			flush();
		}finally{
			for (Transmitter trans : transmitters){
				trans.flagClose();
			}
		}
		boolean oneIsAlive = true;
		while(oneIsAlive){
			oneIsAlive = false;
			for (Transmitter trans : transmitters)
				if (!trans.isTerminated())
					oneIsAlive = true;
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		JSONObject opening = null;
		opening = Transmitter.getJSON(serverAddress+"/finalize?name="+nameCollection);
		if (opening.getBoolean("success") == false){
			throw new IOException("Exception while finalizing collection. Server says:" +opening.getString("message"));
		}
	}
	

	
	
	public static void main(String[] args) throws Exception {
		
		long start = System.nanoTime();
		try(RemoteFrohmdMapBuilder rfmb = new RemoteFrohmdMapBuilder("http://localhost:6800", "test", true);){
			List<Integer> batch = new ArrayList<>();
			for (int i=0; i<100_000_000; i++){
				if (i%100000 == 0)
					System.out.println(i+" elements injected");
				batch.add(i);
				if (batch.size()==10000){
					batch.parallelStream().forEach(in -> {
						try {
							rfmb.put("key"+in, generateVal(in));
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
					batch = new ArrayList<>();
				}
			}
		}
		long end = System.nanoTime();
		System.out.println(end-start);
	}

	public static String generateVal(int keyId){
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<50; i++){
			sb.append("ghofheakfa");
			sb.append(String.valueOf(i));
		}
		return sb.toString();
		
	}
}
