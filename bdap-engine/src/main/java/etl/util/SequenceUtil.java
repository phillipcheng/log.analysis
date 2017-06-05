package etl.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceUtil {
	private static ConcurrentHashMap<String, AtomicLong> sequences=new ConcurrentHashMap<String,AtomicLong>();
	
	public static long getSequenceNum(String type){
		AtomicLong sequence=sequences.get(type);
		if(sequence==null){
			sequence=new AtomicLong(0);
			sequences.putIfAbsent(type, sequence);
			sequence=sequences.get(type);
		}
		
		long value=sequence.incrementAndGet();
		while(value<0){
			sequence.compareAndSet(value, 0);
			value=sequence.incrementAndGet();
		}
		return value;
	}
	
	public static void main(String[] args){
		long a=SequenceUtil.getSequenceNum("a");
		System.out.println(a);
		a=SequenceUtil.getSequenceNum("b");
		System.out.println(a);
		a=SequenceUtil.getSequenceNum("a");
		System.out.println(a);
	}
}
