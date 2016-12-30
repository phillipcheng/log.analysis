package etl.cmd.testcmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import scala.Tuple2;

public class SampleSequenceFileCmd extends ETLCmd {
	public static final Logger logger = LogManager.getLogger(SampleSequenceFileCmd.class);
	private static final long serialVersionUID = 1L;

	public SampleSequenceFileCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
	}

	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		Map<String, Object> retMap = new HashMap<String, Object>();
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		vl.add(new Tuple2<String, String>(Long.toString(offset), row));
		retMap.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return retMap;
	}
	
	
}
