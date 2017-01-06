package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;
import scala.Tuple3;

public class CsvSplitCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvSplitCmd.class);

	private static final @ConfigKey String cfgkey_SPLIT_KEYS = "split.keys";
	private static final @ConfigKey(type=Boolean.class) String cfgkey_SPLIT_KEYS_OMIT = "split.keys.omit";
	private static final @ConfigKey String cfgkey_SPLIT_KEYS_REDUCE_EXP = "split.keys.reduce.exp";
	private static final @ConfigKey(type=Boolean.class) String cfgkey_input_endwithcomma = "input.endwithcomma";

	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	
	private List<IdxRange> splitKeys;
	private boolean splitKeysOmit;
	private boolean inputEndWithComma;
	
	private transient CompiledScript splitKeysReduce;
	
	public CsvSplitCmd(){
		super();
	}
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		
		splitKeys = IdxRange.parseString(getCfgString(cfgkey_SPLIT_KEYS, null));
		splitKeysOmit = getCfgBoolean(cfgkey_SPLIT_KEYS_OMIT, false);
		inputEndWithComma = getCfgBoolean(cfgkey_input_endwithcomma, false);
		
		String cfg = getCfgString(cfgkey_SPLIT_KEYS_REDUCE_EXP, null);
		if (cfg != null && cfg.length() > 0)
			splitKeysReduce = ScriptEngineUtil.compileScript(cfg);
	}
	
	private Tuple2<String, String> getTuple2(CSVRecord csv, List<IdxRange> splitKeys, boolean splitKeysOmit) throws Exception {
		List<String> keys = new ArrayList<String>();
		StringBuilder buffer = new StringBuilder();
		CSVPrinter printer = null;
		String keysName;

		try {
			int i;
			
			printer = new CSVPrinter(buffer, CSVFormat.DEFAULT.withTrim());
			
			for (i = 0; i < csv.size(); i ++) {
				if (isSplitKey(splitKeys, csv.size(), i)) {
					keys.add(tryEscapeHyphen(csv.get(i)));
					if (!splitKeysOmit)
						printer.print(csv.get(i));
					
				} else {
					printer.print(csv.get(i));
				}
			}
			
		} finally {
			if (printer != null)
				printer.close();
		}
		
		keysName = StringUtils.join(keys, "-");
		
		return new Tuple2<String, String>(keysName, buffer.toString());
	}

	private String tryEscapeHyphen(String key) {
		if (key != null && key.contains("-"))
			return key.replace('-', '_');
		else
			return key;
	}

	private boolean isSplitKey(List<IdxRange> splitKeys, int recordValues, int i) {
		for (IdxRange ir: splitKeys) {
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd() == -1) {
				end = recordValues - 1;
			}
			if (i >= start && i <= end)
				return true;
		}
		return false;
	}

	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception{
		super.init();
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		CSVParser parser = null;
		try {
			parser = CSVParser.parse(value, CSVFormat.DEFAULT.withTrim().withTrailingDelimiter(inputEndWithComma));
			for (CSVRecord csv : parser.getRecords())
				vl.add(getTuple2(csv, splitKeys, splitKeysOmit));
		} finally {
			if (parser != null)
				parser.close();
		}
		return vl;
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		Map<String, Object> ret = new HashMap<String, Object>();
		List<Tuple2<String, String>> vl = flatMapToPair(null, row, context);
		ret.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return ret;
	}
	
	private String reduceKey(String key, String value) throws Exception {
		if (splitKeysReduce != null) {
			String[] keys;
			if (key != null && key.length() > 0)
				keys = key.split("-");
			else
				keys = EMPTY_STRING_ARRAY;
			getSystemVariables().put("keys", keys);

			CSVParser parser = null;
			try {
				parser = CSVParser.parse(value, CSVFormat.DEFAULT.withTrim());
				for (CSVRecord csv : parser.getRecords())
					getSystemVariables().put(ETLCmd.VAR_FIELDS, Lists.newArrayList(csv));
			} finally {
				if (parser != null)
					parser.close();
			}
			String result = ScriptEngineUtil.eval(splitKeysReduce, getSystemVariables());
			getSystemVariables().remove("keys");
			getSystemVariables().remove(ETLCmd.VAR_FIELDS);
			return result;
		} else {
			return key;
		}
	}
	
	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<Tuple3<String, String, String>> ret = new ArrayList<Tuple3<String, String, String>>();
		Iterator<String> it = values.iterator();
		while (it.hasNext()) {
			String v = it.next().toString();
			ret.add(new Tuple3<String, String, String>(v, null, reduceKey(key.toString(), v)));
		}
		return ret;
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<String[]> ret = new ArrayList<String[]>();

		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			String v = it.next().toString();
			ret.add(new String[]{v, null, reduceKey(key.toString(), v)});
		}
		
		return ret;
	}
}
