package com.diffbot.frohmd;

import java.io.IOException;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;

public class TestDictionary {
	
	public static void main(String[] args) throws IOException {
		String dict_s="";
		for (int i=0; i<100; i++)
			dict_s += "this is a test that I intend to compress blab bla bla"+i;
		String test="this is a test that I intend to compress blab bla bla"+3456787;
		System.out.println(Zstd.compress(test.getBytes(),3).length);
		ZstdDictCompress dict = new ZstdDictCompress(dict_s.getBytes(), 3);
		ZstdDictDecompress dictd = new ZstdDictDecompress(dict_s.getBytes());
		byte[] compressed = Zstd.compress(test.getBytes(), dict);
		System.out.println(new String(Zstd.decompress(compressed, dictd, compressed.length*8)));
		dict.close();
	}

}
