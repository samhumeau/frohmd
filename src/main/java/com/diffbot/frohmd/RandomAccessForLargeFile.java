package com.diffbot.frohmd;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;


/** Allows to do binary search in a possibly very big file  */
public class RandomAccessForLargeFile implements Closeable {
	
    private static final long PAGE_SIZE = Integer.MAX_VALUE;
    private List<MappedByteBuffer> buffers = new ArrayList<MappedByteBuffer>();
     
    File file;
    FileInputStream fs;
    long fileLength=0;    

    public static void main(String[] args) throws IOException {

    }
    
    public RandomAccessForLargeFile(File file, int lengthPrefixB64Index) throws IOException {
    	this.file = file;
    	fs=new FileInputStream(file);
        FileChannel channel = (fs).getChannel(); 
        fileLength=file.length();
        
        long start = 0, length = 0;
        for (long index = 0; start + length < channel.size(); index++) {
            if ((channel.size() / PAGE_SIZE) == index)
                length = (channel.size() - index *  PAGE_SIZE) ;
            else
                length = PAGE_SIZE;
            start = index * PAGE_SIZE;
            buffers.add((int) index, channel.map(MapMode.READ_ONLY, start, length));
        }    
    }
    
    
    public RandomAccessForLargeFile(File file) throws IOException{
    	this(file,0);
    }
    
    public byte getByte(long bytePosition){
    	int page  = (int) (bytePosition / PAGE_SIZE);
        int index = (int) (bytePosition % PAGE_SIZE);
        return buffers.get(page).get(index);
    }
    
    public byte[] getBytes(long position, int length) throws ArrayIndexOutOfBoundsException{
    	if ((position+length) > file.length() || position<0){
    		System.out.println(position+length);
    		System.out.println(file.length());
    		throw new ArrayIndexOutOfBoundsException("Access out of bounds");
    	}
    	
    	int pageStart  = (int) (position / PAGE_SIZE);
        int indexPageStart = (int) (position % PAGE_SIZE);
        
        int pageEnd  = (int) ( (position + length -1) / PAGE_SIZE);
        
        // Simple case, we are at in the same page
        if (pageStart==pageEnd){
        	buffers.get(pageStart).position(indexPageStart);
        	byte[] answer=new byte[length];
        	buffers.get(pageStart).get(answer);
        	return answer;
        }
        else if (pageEnd==(pageStart+1)){
        	buffers.get(pageStart).position(indexPageStart);
        	int nbBytesFromPage1=(int) (PAGE_SIZE-indexPageStart);
        	byte[] answer1=new byte[nbBytesFromPage1];
        	buffers.get(pageStart).get(answer1);
        	
        	int nbBytesFromPage2=length-nbBytesFromPage1;
        	byte[] answer2=new byte[nbBytesFromPage2];
        	buffers.get(pageStart+1).position(0);
        	buffers.get(pageStart+1).get(answer2);
        	
        	byte[] answer=new byte[length];
        	for (int i=0; i<nbBytesFromPage1; i++)
        		answer[i]=answer1[i];
        	for (int i=0; i<nbBytesFromPage2; i++)
        		answer[i+nbBytesFromPage1]=answer2[i];
        	return answer;
        }
        throw new ArrayIndexOutOfBoundsException("Asked length is too big.");
    }
    

    
	@Override
	public void close() throws IOException {
		fs.close();
	}
    
    
}