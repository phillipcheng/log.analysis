package etl.cmd.testcmd;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.types.ProcessMode;
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

	public List<Tuple2<String, String>> flatMapToPair(String tfName, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		int i = row.indexOf("\n");
		if (i > 0)
			vl.add(new Tuple2<String, String>(row.substring(0, i), row.substring(i)));
		else
			vl.add(new Tuple2<String, String>(tfName, row));
		return vl;
	}
}
