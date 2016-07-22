package etl.cmd.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.AggrOperator;
import etl.engine.FileETLCmd;
import etl.util.IdxRange;
import etl.util.Util;

public class CsvAggregateCmd extends FileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvAggregateCmd.class);
	
	public static final String cfgkey_inputfile="input.file";
	public static final String cfgkey_outputfile="output.file";
	public static final String cfgkey_aggr_op="aggr.op";
	public static final String cfgkey_aggr_groupkey="aggr.groupkey";
	
	private AggrOpMap aoMap;
	private List<IdxRange> groupKeys;
	
	public CsvAggregateCmd(String wfid, String staticCfg, String dynCfg, String defaultFs) {
		super(wfid, staticCfg, dynCfg, defaultFs);
		String[] strAggrOpList = pc.getStringArray(cfgkey_aggr_op);
		aoMap = new AggrOpMap(strAggrOpList);
		groupKeys = IdxRange.parseString(pc.getString(cfgkey_aggr_groupkey));
	}
	
	private List<String> getCsvFields(CSVRecord r, List<IdxRange> irl){
		List<String> keys = new ArrayList<String>();
		for (IdxRange ir: groupKeys){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = r.size()-1;
			}
			for (int i=start; i<=end; i++){
				keys.add(r.get(i));
			}
		}
		return keys;
	}

	@Override
	public Map<String, String> reduceMapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
			List<CSVRecord> csvl = parser.getRecords();
			for (CSVRecord r: csvl){
				List<String> keys = getCsvFields(r, groupKeys);
				String newKey = Util.getCsv(keys, false);
				logger.debug(String.format("new key:%s", newKey));
				context.write(new Text(newKey), new Text(row));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	@Override
	public String reduceProcess(Text key, Iterable<Text> values){
		List<CSVRecord> rl = new ArrayList<CSVRecord>();
		for (Text v: values){
			try {
				CSVParser parser = CSVParser.parse(v.toString(), CSVFormat.DEFAULT);
				rl.addAll(parser.getRecords());
			}catch(Exception e){
				logger.error("", e);
			}
		}
		int idxMax=rl.get(0).size()-1;
		aoMap.constructMap(idxMax);
		List<String> aggrValues = new ArrayList<String>();
		for (int i=0; i<=idxMax; i++){
			AggrOperator op = aoMap.getOp(i);
			if (op!=null){
				if (AggrOperator.sum==op){
					float av=0;
					for (CSVRecord r:rl){
						String strv = r.get(i);
						float v = Float.parseFloat(strv);
						av +=v;
					}
					aggrValues.add(Float.toString(av));
				}else{
					logger.error(String.format("op %s not supported yet.", op.toString()));
				}
			}
		}
		
		return Util.getCsv(aggrValues, false);
	}
}