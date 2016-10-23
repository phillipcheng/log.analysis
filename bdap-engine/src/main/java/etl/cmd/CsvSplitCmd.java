package etl.cmd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.IdxRange;
import scala.Tuple2;

public class CsvSplitCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvSplitCmd.class);

	private static final String SPLIT_KEYS = "split.keys";
	private static final String SPLIT_KEYS_OMIT = "split.keys.omit";
	
	private List<IdxRange> splitKeys;
	private boolean splitKeysOmit;
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs,
			String[] otherArgs) {
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		
		splitKeys = IdxRange.parseString(getCfgString(SPLIT_KEYS, null));
		splitKeysOmit = getCfgBoolean(SPLIT_KEYS_OMIT, false);
	}

	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		Map<String, Object> ret = new HashMap<String, Object>();
		CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT.withTrim());
		CSVRecord csv = parser.getRecords().get(0);
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		vl.add(getTuple2(csv, splitKeys, splitKeysOmit));
		ret.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return ret;
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
					keys.add(csv.get(i));
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

	public List<String[]> reduceProcess(Text key, Iterable<Text> values) throws Exception {
		List<String[]> ret = new ArrayList<String[]>();

		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			String v = it.next().toString();
			ret.add(new String[]{v, null, key.toString()});
		}
		
		return ret;
	}
}
