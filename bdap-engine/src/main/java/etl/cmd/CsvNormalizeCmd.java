package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import etl.engine.LogicSchema;
import etl.engine.ProcessMode;
import scala.Tuple2;

public class CsvNormalizeCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	private static final char DELIMITER = CSVFormat.DEFAULT.getDelimiter();

	public CsvNormalizeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public CsvNormalizeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public CsvNormalizeCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs,
			ProcessMode pm) {
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
	}
	
	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		Map<String, Object> ret = new HashMap<String, Object>();
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		vl.add(new Tuple2<String, String>(inputFileName, row));
		ret.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return ret;
	}

	public List<String[]> reduceProcess(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		List<String[]> ret = new ArrayList<String[]>();
		String inputFileName = key.toString();
		String tableName = getTableNameSetFileName(inputFileName);
		LogicSchema logicSchema = getLogicSchema();
		List<String> attributes = logicSchema.getAttrNames(tableName);
		CSVParser parser = null;
		CSVRecord csv;
		
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			String v = it.next().toString();
			
			try {
				parser = CSVParser.parse(v, CSVFormat.DEFAULT.withTrim());
				csv = parser.getRecords().iterator().next();
				if (csv != null) {
					v = v + delimiters(attributes.size() - csv.size());
					ret.add(new String[]{v, null, tableName});
				} else {
					logger.error("No csv parsed: {}", v);
				}
			} finally {
				if (parser != null) {
					parser.close();
					parser = null;
				}
			}
		}
		
		return ret;
	}

	private String delimiters(int count) {
		if (count > 0) {
			StringBuilder strbuf = new StringBuilder();
			int i;
			for (i = 0; i < count; i++)
				strbuf.append(DELIMITER);
			return strbuf.toString();
		} else {
			return "";
		}
	}

	public boolean hasReduce() {
		return true;
	}
}
