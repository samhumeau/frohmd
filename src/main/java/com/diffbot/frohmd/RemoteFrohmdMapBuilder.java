package com.diffbot.frohmd;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONObject;

import com.google.common.primitives.Ints;


public class RemoteFrohmdMapBuilder implements Closeable{
	String serverAddress, nameCollection;
	boolean compressionForStorage;
	
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	
	LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
	boolean closed = false;
	List<Exception> exceptionRaised = new ArrayList<>();
	AtomicBoolean senderThreadDied = new  AtomicBoolean(false);
	Thread senderThread = new Thread(){
		public void run() {
			while(!closed || queue.size()!=0){
				if (queue.size() != 0){
					try {
						byte[] peek = queue.peek();
						byte[] compressed = Compression.compress(peek);
						postContent(serverAddress+"/put/"+nameCollection, compressed);
					} catch (Exception e) {
						e.printStackTrace();
						exceptionRaised.add(e);
						queue.remove();
						break;
					}
					queue.remove();
				}
				else{
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			senderThreadDied.set(true);
		};
	};
	
	public RemoteFrohmdMapBuilder(String serverAddress, String nameCollection, boolean compressionForStorage) throws IOException{
		this.serverAddress = serverAddress;
		this.nameCollection = nameCollection;
		this.compressionForStorage = compressionForStorage;
		
		JSONObject opening = null;
		opening = getJSON(serverAddress+"/add?name="+nameCollection+"&compression="+compressionForStorage);
		if (opening.getBoolean("success") == false){
			throw new IOException("Exception while opening the connection to remote Frohmd server. Server says:" +opening.getString("message"));
		}
		senderThread.start();
	}
	
	public void flush() throws IOException{
		while(queue.size()>0){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (exceptionRaised.size()>0)
				throw new IOException(exceptionRaised.get(0).getMessage());
		}
		baos.close();
		queue.offer(baos.toByteArray());
		baos = new ByteArrayOutputStream();
		
	}
	
	public void put(String key, String value) throws Exception{
		if (exceptionRaised.size()>0)
			throw exceptionRaised.get(0);
		byte[] key_b = FrohmdMapBuilder.stringToBytes(key);
		byte[] value_b = FrohmdMapBuilder.stringToBytes(value);
		baos.write(Ints.toByteArray(key_b.length));
		baos.write(key_b);
		baos.write(Ints.toByteArray(value_b.length));
		baos.write(value_b);
		if (baos.size() > 5_000_000){
			flush();
		}
	}

	@Override
	public void close() throws IOException {
		System.out.println("closing");
		flush();
		closed = true;
		while(!senderThreadDied.get()){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		JSONObject opening = null;
		opening = getJSON(serverAddress+"/finalize?name="+nameCollection);
		if (opening.getBoolean("success") == false){
			throw new IOException("Exception while finalizing collection. Server says:" +opening.getString("message"));
		}
	}
	
	public static String getContent(String url_string) throws IOException {
		return getContent(url_string, -1);
	}

	public static String getContent(String url_string, int timeout) throws IOException {
		URL url;
		url = new URL(url_string);
		URLConnection con = url.openConnection();
		con.setRequestProperty("User-Agent", "Mozilla/5.0");
		if (timeout>0){
			con.setConnectTimeout(timeout);
			con.setReadTimeout(timeout);
		}
		
		StringBuilder sb=new StringBuilder();
		InputStream is = con.getInputStream();
		InputStreamReader isr=new InputStreamReader(is);
		try(BufferedReader br = new BufferedReader(isr)){
			String input;
			while ((input = br.readLine()) != null) {
				sb.append(input+"\n");
			}
		}
		return sb.toString();

	}
	
	public static JSONObject postContent(String url_string, byte[] data) throws IOException {
		URL url;
		url = new URL(url_string);
		URLConnection con = url.openConnection();
		con.setRequestProperty("User-Agent", "Mozilla/5.0");
		con.setDoOutput(true);
		
		try(OutputStream os = con.getOutputStream()){
			os.write(data);
		}
		con.getOutputStream().close();
		
		StringBuilder sb=new StringBuilder();
		InputStream is = con.getInputStream();
		InputStreamReader isr=new InputStreamReader(is);
		try(BufferedReader br = new BufferedReader(isr)){
			String input;
			while ((input = br.readLine()) != null) {
				sb.append(input+"\n");
			}
		}
		return new JSONObject(sb.toString());

	}

	public static JSONObject getJSON(String url) throws IOException{
		String content = getContent(url);
		return new JSONObject(content);
	}
	
	
	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();
		try(RemoteFrohmdMapBuilder rfmb = new RemoteFrohmdMapBuilder("http://localhost:6800", "test2", true);){
			for (int i=0; i<100_000_000; i++){
				if (i%100000 == 0)
					System.out.println(i);
				rfmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
			}
		}
		long end = System.nanoTime();
		System.out.println(end-start);
	}

}
