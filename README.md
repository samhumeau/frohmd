# frohmd
Fast Read Only HashMap on Disk

### Description

This is a java implementation of a fast read only hash map in Java. 

Pro: 
- It is fast to read (about 4,000 readings per second)
- Very fast to write (about 1,000,000 writings per second)
- It is purely on disk, No loading time.
- Efficient on disk space.
- It self contain a little http server that allows you to easily publish your maps.

Cons:
- It is read only, you build it in a batch.
- Duplicate keys may results in using more disk than necessary
- The key is dropped, a 64 hash of the key is considered as the key.

### Basic Usage

How to write:
```java
try(FrohmdMapBuilder fmb=new FrohmdMapBuilder("testIndex");){
	for (int i=0; i<250_000_000; i++){
		fmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
	}
}
```
The following code runs in 5mn on a modern laptop with Intel SSD (about 1,000,000 writes per seconds). The result is a 30GB files, way bigger than my 16GB of RAM.

How to read:
```java
try(FrohmdMap map=new FrohmdMap("testIndex");){
	String s=map.getString("key1");
}
```
On an XPS 15 laptop (2014) and a single SSD I obtain 4000 random reads per second.


### Compression

You can beneficiate from Zstd dictionary compression using the second parameter of the builder. The compression dictionary is build on the 100 first samples. This will slow down insertion dramatically but will use much less space on disk
```java
try(FrohmdMapBuilder fmb=new FrohmdMapBuilder("testIndex", true);){
	for (int i=0; i<250_000_000; i++){
		fmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
	}
}
```

### Using the server

Frohmd contains a small tomcat based server. To run it as a server, clone this repository, have java 8 installed and run 
```bash
./gradlew server
```
The server should start on defaut port 6800. You can check the status of the server by going to localhost:6800
Configuration is done in 'conf/frohmd.toml':
```toml
# Frohmd configuration
title = "Frohmd configuration"
[server]
port = 6800
shards = ["./data/shard1","./data/shard2"]

```
Where you can specify the port and also the location on disk of the data with 'shards', for example if you want to distribute the data on several disks. Having multiple shards will also allow to distribute the computation.

### uploading to the server

You upload a collection from java with the RemoteFrohmdMapBuilder class
```java
try(RemoteFrohmdMapBuilder rfmb = new RemoteFrohmdMapBuilder("http://localhost:6800", "collectionName", false);){
	for (int i=0; i<1_000_000; i++){
		rfmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
	}
}
```

Once it's done, you can search on the server using the endoint search:
http://localhost:68000/search?collection=collectionName&keys=key1,key2
should return 
```json
{
	success: true,
	timeSpentMs: 1,
	results: {
		key1: "This is the value (and it is quite a very very very long value) for the key. 1",
		key2: "This is the value (and it is quite a very very very long value) for the key. 2"
	}
}
```
