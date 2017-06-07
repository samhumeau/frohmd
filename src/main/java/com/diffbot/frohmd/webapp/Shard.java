package com.diffbot.frohmd.webapp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.diffbot.frohmd.FrohmdMap;
import com.diffbot.frohmd.FrohmdMapBuilder;
import com.diffbot.frohmd.IndexLine;

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
		System.out.println(compress);
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
	
	
	
	/***************************
	 * 
	 *  Status
	 *************************/
	
	
	public static class CollectionStatus{
		String status = "FINALIZED";
		String name = "zz";
		long numberOfKeys;
		long sizeDataByte;
		boolean isCompressed;
		
		public void merge(CollectionStatus otherCollectionStatus){
			if (name.equals(otherCollectionStatus.name)){
				if ("GETTINGDATA".equals(status) || "GETTINGDATA".equals(otherCollectionStatus))
					status = "GETTINGDATA";
				else if ("INDEXING".equals(status) || "INDEXING".equals(otherCollectionStatus))
					status = "INDEXING";
				else
					status = "FINALIZED";
				numberOfKeys += otherCollectionStatus.numberOfKeys;
				sizeDataByte += otherCollectionStatus.sizeDataByte;
				if (isCompressed || otherCollectionStatus.isCompressed)
					isCompressed = true;
			}
		}
		@Override
		public String toString() {
			return "<tr><td>"+name+"</td><td>"+status+"</td><td>"+(numberOfKeys* IndexLine.sizeLine + sizeDataByte)/1000000+"MB</td><td>"+isCompressed+"</td></tr>";
		}
		public static String getHeader(){
			return "<tr><th>name</th><th>status</th><th>total size on disk</th><th>compressed</th></tr>";
		}
	}
	
	public static class Status{
		String status = "ok";
		String folderOnDisk;
		List<CollectionStatus> collections = new  ArrayList<>();
	}
	
	public Status status(){
		Status answer = new Status();
		answer.folderOnDisk = shardFolder;
		for (String key : openedBuilders.keySet()){
			CollectionStatus col = new CollectionStatus();
			col.status = "GETTINGDATA";
			col.name = key;
			col.numberOfKeys = openedBuilders.get(key).nbKeys;
			col.sizeDataByte = openedBuilders.get(key).sizeRecord;
			col.isCompressed = openedBuilders.get(key).isCompress();
			answer.collections.add(col);
		}
		for (String key : indexedBuilders.keySet()){
			CollectionStatus col = new CollectionStatus();
			col.status = "INDEXING";
			col.name = key;
			col.numberOfKeys = indexedBuilders.get(key).nbKeys;
			col.sizeDataByte = indexedBuilders.get(key).sizeRecord;
			col.isCompressed = indexedBuilders.get(key).isCompress();
			answer.collections.add(col);
		}
		for (String key : openedReaders.keySet()){
			CollectionStatus col = new CollectionStatus();
			col.status = "FINALIZED";
			col.name = key;
			col.numberOfKeys = openedReaders.get(key).nbKeys;
			col.sizeDataByte = openedReaders.get(key).sizeData;
			col.isCompressed = openedReaders.get(key).isCompress();
			answer.collections.add(col);
		}
		return answer;
	}
}
