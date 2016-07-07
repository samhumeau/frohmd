package com.diffbot.frohmd;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/** A Frohmd Index is ideal when you need the structure 
 *  String -> String[]
 *  
 *  It allows you to add values online:
 *  
 *  indexBuilder=new FrohmdIndexBuilder(pathToFile);
 *  indexBuilder.add("key","value1");
 *  indexBuilder.add("key","value2");
 *  indexBuilder.close();
 *  
 *  index=new FrohmdIndex(pathToFile);
 *  index.get("key") // return ["value1","value2"]
 *  index.close();
 *  
 *  @author sam
 */
public class FrohmdIndex implements Closeable{
	public static final Charset charset=Charset.forName("UTF-8");
	FrohmdMap map;
	
	public FrohmdIndex(String file) throws IOException {
		map=new FrohmdMap(file);
	}
	
	/** return the array of String corresponding to the given key. 
	 *  Return null if the index does not contain this key.
	 * @param key
	 * @return
	 */
	public String[] get(String key){
		byte[] b_key=key.getBytes(charset);
		byte[] pack_=map.get(b_key);
		if (pack_==null)
			return null;
		
		byte[][] packs=BytePackager.unpack(pack_);
		String[] answer=new String[packs.length];
		for (int i=0; i<packs.length; i++)
			answer[i]=new String(packs[i], charset);
		return answer;
	}
	
	@Override
	public void close() throws IOException {
		map.close();
	}
	
	public static void main(String[] args) throws IOException {
		FrohmdIndexBuilder indexBuilder=new FrohmdIndexBuilder("testInd3");
		long start=System.currentTimeMillis();
		for (int i=0; i<500_000_000; i++){
			int v=i/2500;
			int key=i % 25_000_000;
			indexBuilder.add(String.valueOf(key), String.valueOf(v));
			if (i % 1_000_000 == 0){
				System.out.println("Prepared "+i+" elements after "+(System.currentTimeMillis()-start)+"ms (current example: "+key+","+v);
			}
		}
		start=System.currentTimeMillis();
		indexBuilder.close();
		System.out.println("Building the index properly talking took "+(System.currentTimeMillis()-start)+"ms");
		
		FrohmdIndex fi=new FrohmdIndex("testInd3");
		System.out.println(Arrays.toString(fi.get("100000")));
		fi.close();
		
	}

}
