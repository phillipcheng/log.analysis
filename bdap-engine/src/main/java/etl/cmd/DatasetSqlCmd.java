package etl.cmd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import etl.cmd.transform.TableIdx;
import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.DBUtil;
import etl.util.IdxRange;
import etl.util.SchemaUtils;
import scala.Tuple2;

/*****
 * DataSet transform
 */
public class DatasetSqlCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(DatasetSqlCmd.class);
	
	public static final String no_table_value="no";
	public static final String key_idx="idx";

	public static final @ConfigKey String cfgkey_sqls = "sqls";
	public static final @ConfigKey String cfgkey_new_tables = "new.tables";
	
	private String[] sqls;
	private String[] newtables;
	private Map<String, TableIdx> idxMap;
	
	//for serialization
	public DatasetSqlCmd(){
		super();
	}
	
	//spark code generation
	public DatasetSqlCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		sqls = super.getCfgStringArray(cfgkey_sqls);
		newtables = super.getCfgStringArray(cfgkey_new_tables);
		idxMap = new HashMap<String, TableIdx>();
		Iterator it = super.getPc().getKeys(key_idx);
		while (it.hasNext()){
			String key = (String)it.next();
			String name = key.substring(key.indexOf(key_idx)+key_idx.length()+1);
			String nidxExp = super.getPc().getString(key);
			int dotIdx = nidxExp.indexOf(".");
			String tn = ETLCmd.SINGLE_TABLE;
			if (dotIdx!=-1){
				tn = nidxExp.substring(0, dotIdx);
			}
			String idxExp = nidxExp.substring(dotIdx+1); 
			idxMap.put(name, new TableIdx(tn, IdxRange.parseString(idxExp)));
		}
		logger.info(String.format("idxMap:%s", idxMap));
	}
	
	@Override
	public boolean useSparkSql(){
		return true;
	}
	
	@Override
	public JavaPairRDD<String,String> dataSetProcess(JavaSparkContext jsc, SparkSession spark, int singleTableColNum){
		for (String vn: idxMap.keySet()){
			TableIdx ti = idxMap.get(vn);
			if (ETLCmd.SINGLE_TABLE.equals(ti.getTableName())){
				ti.setColNum(singleTableColNum);
			}else{
				ti.setColNum(getLogicSchema().getAttrNames(ti.getTableName()).size());
			}
		}
		JavaPairRDD<String,String> ret = JavaPairRDD.fromJavaRDD(jsc.emptyRDD());
		for (int i=0; i<sqls.length; i++){
			String rawSql = sqls[i];
			String cookedSql=DBUtil.updateVar(rawSql, idxMap, this.getLogicSchema());
			Dataset<Row> r = spark.sql(cookedSql);//execute sql
			String newTable = newtables.length==0?ETLCmd.SINGLE_TABLE:newtables[i];
			if (!no_table_value.equals(newTable)){//output
				JavaPairRDD<String,String> res = r.javaRDD().mapToPair(new PairFunction<Row, String,String>(){
					private static final long serialVersionUID = 1;
					@Override
					public Tuple2<String, String> call(Row t) throws Exception {
						String v = SchemaUtils.convertToString(t, csvValueSep);
						return new Tuple2<String, String>(newTable, v);
					}
				});
				ret = ret.union(res);
			}
		}
		return ret;
	}
}
