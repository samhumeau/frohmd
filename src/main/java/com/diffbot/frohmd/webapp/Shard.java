package com.diffbot.frohmd.webapp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import com.diffbot.frohmd.FrohmdMap;
import com.diffbot.frohmd.FrohmdMapBuilder;

public class Shard {

	Map<String, FrohmdMapBuilder> openedBuilders = new  HashMap<>();
	Map<String, FrohmdMap> openedReaders = new  HashMap<>();
	Map<String, FrohmdMapBuilder> indexedBuilders = new  HashMap<>();
	String shardFolder;
	
	public Shard(String shardFolder) throws IOException{
		this.shardFolder = shardFolder;
		new File(shardFolder).mkdirs();
		File[] subF = new File(shardFolder).listFiles();
		for (File f : subF){
			if (f.isDirectory()){
				if (hasBeenFinalized(f.getName())){
					openedReaders.put(f.getName(), new FrohmdMap(f.getAbsolutePath()+"/base"));
				}
			}
		}
	}
	
	private boolean hasBeenFinalized(String collection){
		File finalizedFlag = new File(shardFolder+"/"+collection+"/finalized.flag");
		if (finalizedFlag.exists())
			return true;
		return false;
	}
	
	public void openCollection(String collection, boolean compress) throws IOException{
		if (openedReaders.containsKey(collection)){
			openedReaders.get(collection).close();
			openedReaders.remove(collection);
		}
		if (openedBuilders.containsKey(collection)){
			openedBuilders.get(collection).close();
			openedBuilders.remove(collection);
		}
		File ff = new File(shardFolder+"/"+collection);
		if (ff.exists() && ff.isFile())
			ff.delete();
		if (ff.exists() & ff.isDirectory())
			FileUtils.deleteDirectory(ff);
		ff.mkdirs();
		openedBuilders.put(collection, new FrohmdMapBuilder(ff.getAbsolutePath()+"/base", compress));
	}
	
	public void addData(String collection, byte[] key, byte[] value) throws IOException{
		if (!openedBuilders.containsKey(collection))
			throw new IOException("No such collection");
		openedBuilders.get(collection).put(key, value);
	}
	
	public byte[] getData(String collection, byte[] key) throws IOException{
		if (!openedReaders.containsKey(collection))
			throw new IOException("No such collection");
		return openedReaders.get(collection).get(key);
	}
	
	public void finalize(String collection) throws IOException{
		if (!openedBuilders.containsKey(collection))
			throw new IOException("No such collection");
		FrohmdMapBuilder fmb = openedBuilders.remove(collection);
		indexedBuilders.put(collection, fmb);
		fmb.close();
		indexedBuilders.remove(collection);
		File ff = new File(shardFolder+"/"+collection);
		FileUtils.touch(new File(shardFolder+"/"+collection+"/finalized.flag"));
		openedReaders.put(collection, new FrohmdMap(ff.getAbsolutePath()+"/base"));
	}
	
	
	public JSONObject status(){
		JSONObject jo = new JSONObject();
		jo.put("status", "ok");
		JSONObject collections = new JSONObject();
		for (String key : openedBuilders.keySet()){
			JSONObject col = new JSONObject();
			col.put("status", "GETTINGDATA");
			col.put("name", key);
			col.put("number of keys", openedBuilders.get(key).nbKeys);
			col.put("size data (byte)", openedBuilders.get(key).sizeRecord);
			collections.put(key, col);
		}
		for (String key : indexedBuilders.keySet()){
			JSONObject col = new JSONObject();
			col.put("status", "INDEXING");
			col.put("name", key);
			col.put("number of keys", indexedBuilders.get(key).nbKeys);
			col.put("size data (byte)", indexedBuilders.get(key).sizeRecord);
			collections.put(key, col);
		}
		for (String key : openedReaders.keySet()){
			JSONObject col = new JSONObject();
			col.put("status", "FINALIZED");
			col.put("name", key);
			col.put("number of keys", openedReaders.get(key).nbKeys);
			col.put("size data (byte)", openedReaders.get(key).sizeData);
			collections.put(key, col);
		}
		jo.put("collections", collections);
		jo.put("dedicated folder", shardFolder);
		return jo;
	}
}
