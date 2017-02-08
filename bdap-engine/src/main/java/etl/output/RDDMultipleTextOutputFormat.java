package etl.output;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat<String, String>{
	
	@Override
	public String generateFileNameForKeyValue(String key, String value, String name) {
		return key + '-' + name;
	}
	
	@Override
	protected String generateActualKey(String key, String value) {
		return null;
	}
}
