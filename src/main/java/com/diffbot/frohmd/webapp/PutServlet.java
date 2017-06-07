package com.diffbot.frohmd.webapp;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.diffbot.frohmd.Compression;
import com.google.common.primitives.Ints;

public class PutServlet extends HttpServlet{
	
		public static class Batch{
			List<byte[]> keys = new ArrayList<>();
			List<byte[]> values = new ArrayList<>();
			Shard shard;
			
			public Batch(Shard s){
				this.shard =s;
			}
			
			public void add(byte[] key, byte[] value){
				keys.add(key);
				values.add(value);
			}
		}
		private static final long serialVersionUID = 8264419907955L;

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
	        resp.setHeader("Access-Control-Allow-Origin", "*");
	        resp.setContentType("application/json;charset=UTF-8");
	        
	        
	        String nameCollection = req.getRequestURL().toString().substring(req.getRequestURL().toString().lastIndexOf("/")+1);
			if (nameCollection == null || nameCollection.length() ==0){
				AddCollectionServlet.sendError("No name of collection specified", resp);
				return;
			}
			
			List<Batch> batches = new ArrayList<Batch>();
			for (Shard shard : Server.shards)
				batches.add(new Batch(shard));
			
			byte[] intBuffer = new byte[4];
			byte[] longBuffer = new byte[1024];
			byte[] compressed = Compression.toByteArray(req.getInputStream());
			byte[] block = Compression.uncompress(compressed);
			
			
			
			boolean first = true;
			try(ByteArrayInputStream io = new ByteArrayInputStream(block)){
				while(true){
					int lengthKey = nextInt(intBuffer, io);
					if (lengthKey==-1)
						break;
					byte[] key = nextArray(lengthKey, io, longBuffer);
					int lengthValue = nextInt(intBuffer, io);
					byte[] value = nextArray(lengthValue, io, longBuffer);
					int id = Server.keyToShard(key);
					batches.get(id).add(key, value);
					if (first){
						first = false;
					}
				}
			}
			
			List<Exception> exceptionRaised = new ArrayList<>();
			batches.parallelStream().forEach(b -> {
				for(int i=0; i<b.keys.size(); i++)
					try {
						b.shard.addData(nameCollection, b.keys.get(i), b.values.get(i));
					} catch (IOException e) {
						e.printStackTrace();
						break;
					}
			});
			
			if (exceptionRaised.size() ==0 )
				AddCollectionServlet.sendSuccess("success", resp);
			else
				AddCollectionServlet.sendError(exceptionRaised.get(0).getMessage(), resp);
		}
		
		// return the next int of the stream. or -1 if EOF
		public static int nextInt(byte[] buffer, InputStream stream) throws IOException{
			int read = stream.read(buffer);
			if (read<4)
				return -1;
			return Ints.fromByteArray(buffer);
		}
		
		public static byte[] nextArray(int length, InputStream stream, byte[] buffer) throws IOException{
			byte[] bytes = new byte[length];
			int read =0;
			while (read<length){
				int addread = stream.read(buffer, 0, Math.min(length-read, buffer.length));
				for (int i=0; i<addread; i++)
					bytes[read+i] = buffer[i];
				read += addread;
			}
			return bytes;
		}
		
}
