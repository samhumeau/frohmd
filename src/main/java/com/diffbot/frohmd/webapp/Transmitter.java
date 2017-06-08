package com.diffbot.frohmd.webapp;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.JSONObject;

import com.diffbot.frohmd.Compression;

/** A transmitter thread, used for the remote Frohmd Map Builder */
public class Transmitter extends Thread{
	boolean available = true;
	boolean exeption = false;
	boolean closed = false;
	boolean terminated = false;
	String serverAddress, nameCollection;
	ConcurrentLinkedQueue<byte[]> toTransmit = null;
	
	
	public Transmitter(String serverAddress, String nameCollection) {
		this.serverAddress = serverAddress;
		this.nameCollection = nameCollection;
	}
	
	public boolean isAvailable(){
		return available;
	}
	
	public void flagClose(){
		closed = true;
	}
	
	public boolean transmitIfAvailable(ConcurrentLinkedQueue<byte[]> bytes){
		if (available){
			synchronized (this) {
				if (available){
					available = false;
					toTransmit = bytes;
					return true;
				}
				else{
					return false;
				}
			}
		}
		return false;
	}
	
	public boolean encounteredProblem(){
		return exeption;
	}
	
	public boolean isTerminated(){
		return terminated;
	}
	
	@Override
	public void run() {
		while(!closed || toTransmit!=null){
			if (toTransmit!=null){
				try{
					byte[] next = toTransmit.poll();
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					while(next!=null){
						baos.write(next);
						next = toTransmit.poll();
					}
					
					byte[] compressed = Compression.compress(baos.toByteArray());
					postContent(serverAddress+"/put/"+nameCollection, compressed);
					available = true;
				}catch(Exception e){
					e.printStackTrace();
					exeption = true;
				}
				toTransmit = null;
				available = true;
			}
			else{
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		terminated = true;
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
	
	public static byte[] concatenate(byte[] ... bytes){
		int sizeAnswer =0;
		for (byte[] bs : bytes)
			sizeAnswer+=bs.length;
		byte[] answer = new byte[sizeAnswer];
		int counter=0;
		for (byte[] bs : bytes)
			for (int i=0; i<bs.length; i++){
				answer[counter] = bs[i];
				counter++;
			}
		return answer;
	}
}