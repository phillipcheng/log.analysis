package etl.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class TableNotEqualsFilterFun implements Function<Tuple2<String,String>, Boolean>{
	private static final long serialVersionUID = 1L;
	private List<String> tableNames;
	
	public TableNotEqualsFilterFun(String[] tableNames){
		this.tableNames = Arrays.asList(tableNames);
	}
	
	@Override
	public Boolean call(Tuple2<String, String> v1) throws Exception {
		if (!tableNames.contains(v1._1)){
			return true;
		}else{
			return false;
		}
	}
}