package etl.spark;

import java.util.List;

public interface SparkReciever {
	/**
	 * 
	 * @param input
	 * @return
	 */
	public List<String> sparkRecieve();
}
