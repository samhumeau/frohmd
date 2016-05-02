# frohmd
Fast Read Only HashMap on Disk

### Description

This is a java implementation of a fast read only hash map in Java. 
Pro: 
- It is fast to read (about 4,000 readings per second)
- Very fast to write (about 1,000,000 writings per second)
- It is purely on disk, No loading time.
- Efficient on disk space.
Cons:
- It is read only, you build it in a batch.
- Small probablity of collision (and so error during reading). 
 Right now, with 500 million entries, there is 0.6% chances to observe 1 collision. (and one chance over 20,000 to observe 2 collisions)
- Duplicate keys may results in using more disk than necessary

### Usage

How to write:
```java
FrohmdMapBuilder fmb=new FrohmdMapBuilder("testIndex");
for (int i=0; i<250_000_000; i++){
	fmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
}
fmb.close();
```
The following code runs in 5mn on a modern laptop with Intel SSD (about 1,000,000 writes per seconds). The result is a 30GB files, way bigger than my 16GB of RAM.

How to read:
```java
FrohmdMap map=new FrohmdMap("testIndex");
String s=map.getString("key1");
map.close();
```

Reading speed. The following code evaluates the reading speed:
```java
FrohmdMap map=new FrohmdMap("testIndex");
long start=System.nanoTime();
Random rand=new Random();
int nbError=0;
for (int i=0; i<10000; i++){
	int ri=rand.nextInt(250_000_000);
	String s=map.getString("key"+ri);
	if (!s.endsWith(String.valueOf(ri)))
			nbError++;
}

System.out.println("10,000 random reads in "+(System.nanoTime()-start)/1e6+"ms");
System.out.println(nbError);
map.close();
```
Which on my laptop outputs "10,000 random reads in 2608.152721ms", so about 4000 reads per second.

