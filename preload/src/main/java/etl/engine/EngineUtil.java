package etl.engine;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class EngineUtil {
	public static final Logger logger = Logger.getLogger(EngineUtil.class);
	
	public static void processCmds(ETLCmd[] cmds, long offset, String row, 
			Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws Exception {
		String input = row;
		for (int i=0; i<cmds.length; i++){
			ETLCmd cmd = cmds[i];
			List<String> outputs = cmd.process(offset, input, context);
			if (i<cmds.length-1){//intermediate steps
				if (outputs!=null && outputs.size()==1){
					input = outputs.get(0);
				}else{
					String outputString = "null";
					if (outputs!=null){
						outputString = outputs.toString();
					}
					logger.error(String.format("output from chained cmd should be a string. %s", outputString));
				}
			}else{//last step
				if (outputs!=null){
					if (context!=null){
						for (String line:outputs){
							context.write(new Text(line), NullWritable.get());
						}
					}else{
						logger.info(String.format("final output:%s", outputs));
					}
				}
			}
		}
		
	}

}
