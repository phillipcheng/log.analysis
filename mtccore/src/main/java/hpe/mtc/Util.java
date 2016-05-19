package hpe.mtc;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class Util {
	public static final Logger logger = Logger.getLogger(GenCmd.class);
	
	//k1=v1,k2=v2 =>{{k1,v1},{k2,v2}}
	public static TreeMap<String, String> parseMapParams(String params){
		TreeMap<String, String> paramsMap = new TreeMap<String, String>();
		if (params==null){
			return paramsMap;
		}
		String[] strParams = params.split(",");
		for (String strParam:strParams){
			String[] kv = strParam.split("=");
			if (kv.length<2){
				logger.error(String.format("wrong param format: %s", params));
			}else{
				paramsMap.put(kv[0].trim(), kv[1].trim());
			}
		}
		return paramsMap;
	}
	
	public static String genCreateTableSql(List<String> fieldNameList, List<String> fieldTypeList, String tn){
		StringBuffer tablesql = new StringBuffer();
		//removing .
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			if (name.contains(".")){
				name = name.substring(name.lastIndexOf(".")+1);
				fieldNameList.set(i, name);
			}
		}
		//gen table sql
		tablesql.append(String.format("drop table %s;\n", tn));
		tablesql.append(String.format("create table if not exists %s(\n", tn));
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			String type = fieldTypeList.get(i);
			tablesql.append(String.format("%s %s", name, type));
			if (i<fieldNameList.size()-1){
				tablesql.append(",");
			}
		}
		tablesql.append(");\n");
		return tablesql.toString();
	}
	
	public static String guessType(String value){
		boolean atleastOneAlpha = value.matches(".*[a-zA-Z]+.*");
		if (atleastOneAlpha){
			int len = value.length();
			return String.format("varchar(%d)", Math.max(20, 2*len));
		}else{
			return String.format("numeric(%d,%d)", 15,5);
		}
	}
}
