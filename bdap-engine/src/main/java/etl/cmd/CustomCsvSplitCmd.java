package etl.cmd;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import scala.Tuple2;
import scala.Tuple3;
import etl.engine.ETLCmd;
import etl.engine.EngineUtil;
import etl.engine.types.OutputType;
import etl.engine.types.ProcessMode;

public class CustomCsvSplitCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	
	private String commonTitle = "#NE_NAME,NE_VERSION,CATEGORY,GROUP";
	private Map<String, String> tableIdNameMap = new HashMap<String, String>();

	public CustomCsvSplitCmd() {
	}

	public CustomCsvSplitCmd(String wfName, String wfid, String staticCfg,
			String defaultFs, String[] otherArgs) {
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs,
				ProcessMode.Single);
	}

	public CustomCsvSplitCmd(String wfName, String wfid, String staticCfg,
			String defaultFs, String[] otherArgs, ProcessMode pm) {
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}

	@Override
	public void init(String wfName, String wfid, String staticCfg,
			String prefix, String defaultFs, String[] otherArgs, ProcessMode pm) {
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		tableIdNameMap = logicSchema.getTableIdNameMap();
	}

	@Override
	public Map<String, Object> mapProcess(long offset, String row,
			Mapper<LongWritable, Text, Text, Text>.Context context,
			MultipleOutputs<Text, Text> mos) throws Exception {
		if (SequenceFileInputFormat.class.isAssignableFrom(context
				.getInputFormatClass())) {
			int index = row.indexOf("\n");
			String pathName = row.substring(0, index);
			String tfName = getTableNameSetPathFileName(pathName);

			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName();
			String line = row.substring(index + 1);
			List<Tuple2<String, String>> ret = flatMapToPair(fileName, line,
					context);
			if (ret != null) {
				for (Tuple2<String, String> t : ret) {
					context.write(new Text(t._1), new Text(t._1 + "\n" +t._2));
				}
			}
		} else {
			//nothing to do.
		}

		return null;
	}

	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName,
			String value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws Exception {
		super.init();
		List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
		List<Tuple2<String, String>> csvList = parseCsv2Values(tableName, value, context);
		if (context!=null){
//			int nameNum = 1;
			for(Tuple2<String, String> item: csvList){
				String item2 = item._2;
				int index = item2.indexOf("\n");
				String row = item2.substring(0, index);
				String [] rowArray = row.split(",");
//				String fileName = "";
//				for(int i =0; i < 4; i++){
//					fileName += rowArray[i]+ "-";
//				}
//				fileName = "FGW_EMS_OM_" + nameNum;
//				nameNum ++;
				context.write(new Text(item._1), new Text(item._2));
			}
		}else{
			retList.addAll(csvList);
			return retList;
		}
		return null;
	}
	
	protected List<Tuple2<String, String>> parseCsv2Values(String key, String text,
			Mapper<LongWritable, Text, Text, Text>.Context context) {
		List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
		Map<String, StringBuffer> map = new LinkedHashMap<String, StringBuffer>();
		String[] rows = text.split("\n");
		StringBuffer stringBuff = new StringBuffer();
		String oldTitleRow = "";
		String titleKey = "";
		int rowsCount = rows.length;
		for(int i =0 ; i < rowsCount; i++){
			String row = rows[i];
			for(int j=0; j <= 10; j++){
				row = row.replaceAll(",,", ",");
			}
			
			int titleIndex = row.indexOf(commonTitle);
			if(titleIndex != -1){
				titleKey = row;
				StringBuffer strBuffer = map.get(titleKey);
				if(strBuffer != null){
					stringBuff = strBuffer;
				}else {
					stringBuff = new StringBuffer();
				}
			}else{
				stringBuff.append(row);
				stringBuff.append("\n");
			}
			int nextInt = i + 1;
			if(nextInt < rowsCount){
				String newRow = rows[i+1];
				int newTitleIndex = newRow.indexOf(commonTitle);
				if(newTitleIndex != -1){
					String tableName = tableIdNameMap.get(titleKey.replace("\r", "").replace("\n", ""));
					if(StringUtils.isNotEmpty(tableName)){
						stringBuff.insert(0, titleKey + "\n");
						map.put(tableName, stringBuff);
					}
				}
			}
			
		}
		
		Iterator iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String mapKey = (String)entry.getKey();
			StringBuffer mapVal = (StringBuffer)entry.getValue();
			Tuple2<String, String> tuple = new Tuple2<String, String>(mapKey, mapVal.toString());
			retList.add(tuple);
		}
		return retList;
	}

	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key,
			Iterable<? extends Object> values,
			Reducer<Text, Text, Text, Text>.Context context,
			MultipleOutputs<Text, Text> mos) throws Exception {
		
		String fileName = key.toString();
		Iterator<? extends Object> it = values.iterator();
		List<Tuple3<String, String, String>> ret = new ArrayList<Tuple3<String, String, String>>();
		while (it.hasNext()){
			String v = it.next().toString();
			String tableName = ETLCmd.SINGLE_TABLE;
			if (super.getOutputType()==OutputType.multiple){
				tableName = fileName;
			}
			if (context!=null){//map reduce
				EngineUtil.processReduceKeyValue(v, null, tableName, context, mos);
			}else{
				ret.add(new Tuple3<String, String, String>(v, null, tableName));
			}
		}
		return ret;
		
	}

}
