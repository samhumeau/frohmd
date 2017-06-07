package com.diffbot.frohmd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.primitives.Ints;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

/** Bunch of helpers for compression.
 *  Keep everything in this class this way we can think of multi core compression methods if needed. */
public class Compression {
	
	public static byte[] compress(byte[] array) throws IOException{
		try (ByteArrayInputStream bais = new ByteArrayInputStream(array);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    OutputStream os = new LZ4BlockOutputStream(baos);){
				byte[] buffer = new byte[1024];
				os.write(Ints.toByteArray(array.length));
				int nbRead = bais.read(buffer);
				while(nbRead!=-1){
					os.write(buffer, 0, nbRead);
					nbRead = bais.read(buffer);
				}
				os.flush();
				os.close();
				return baos.toByteArray();
			 }
		
	}
	
	public static byte[] uncompress(byte[] array) throws IOException{
		try (ByteArrayInputStream bais = new ByteArrayInputStream(array);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
			    InputStream is = new LZ4BlockInputStream(bais);){
					byte[] buffer = new byte[1024];
					byte[] intBuff = new byte[4];
					is.read(intBuff);
					int nbBytes = Ints.fromByteArray(intBuff);
					int nbRead = is.read(buffer, 0, Math.min(buffer.length, nbBytes - baos.size()));
					while(nbRead!=-1 && baos.size() != nbBytes){
						baos.write(buffer, 0, nbRead);
						nbRead = is.read(buffer, 0, Math.min(buffer.length, nbBytes - baos.size()));
					}
					return baos.toByteArray();
				 }
	}
	
	public static byte[] toByteArray(InputStream is) throws IOException{
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();){
					byte[] buffer = new byte[1024];
					int nbRead = is.read(buffer);
					while(nbRead!=-1){
						baos.write(buffer, 0, nbRead);
						nbRead = is.read(buffer);
					}
					return baos.toByteArray();
				 }
	}
}
