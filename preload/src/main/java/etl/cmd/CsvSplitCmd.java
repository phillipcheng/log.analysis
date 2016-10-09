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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.IdxRange;
import scala.Tuple2;

public class CsvSplitCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvSplitCmd.class);

	private static final String SPLIT_KEYS = "split.keys";
	
	private List<IdxRange> splitKeys;
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvSplitCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs,
			String[] otherArgs) {
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		
		splitKeys = IdxRange.parseString(super.getCfgString(SPLIT_KEYS, null));
	}

	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		Map<String, Object> ret = new HashMap<String, Object>();
		CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
		CSVRecord csv = parser.getRecords().get(0);
		String key = getCSVKeys(csv, splitKeys);
		Tuple2<String, String> v = new Tuple2<String, String>(key, row);
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		vl.add(v);
		ret.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		return ret;
	}

	private String getCSVKeys(CSVRecord csv, List<IdxRange> splitKeys) {
		List<String> keys = new ArrayList<String>();
		for (IdxRange ir: splitKeys) {
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd() == -1) {
				end = csv.size() - 1;
			}
			for (int i = start; i <= end; i ++) {
				keys.add(csv.get(i));
			}
		}
		return keys.toString();
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
