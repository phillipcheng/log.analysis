package etl.spark;

import org.apache.spark.streaming.receiver.Receiver;

public interface SparkReciever {
	/**
	 * 
	 * @param input
	 * @return
	 */
	public void sparkRecieve(Receiver<String> r);
}
