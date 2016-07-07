package com.diffbot.frohmd;

import java.util.Comparator;

/** A pair of byte array */
public class PairByteArray {
	
	byte[] first;
	byte[] second;
	
	public PairByteArray(byte[] first,byte[] second) {
		this.first=first;
		this.second=second;
	}
	
	/** Compare by the first key */
	public static Comparator<PairByteArray> comparator=new Comparator<PairByteArray>() {
		@Override
		public int compare(PairByteArray o1, PairByteArray o2) {
			byte[] f1=o1.first;
			byte[] f2=o2.first;
			if (f1==null){
				if (f2==null)
					return 0;
				else
					return -1;
			}
			if (f2==null)
				return 1;
			int bound=Math.min(f1.length, f2.length);
			for (int i=0;i<bound; i++){
				if (f1[i]!=f2[i]){
					if (f1[i]>f2[i])
						return 1;
					else
						return -1;
				}
			}
			return Integer.compare(f1.length, f2.length);
		}
	};
}
