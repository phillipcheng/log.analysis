package etl.util;

import java.io.Serializable;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.spark_project.guava.base.Objects;

import etl.engine.SafeSimpleDateFormat;


public class FieldType implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(FieldType.class);
	
	public static final String TYPE_NUMERIC="numeric";
	public static final String TYPE_VARCHAR="varchar";
	public static final String TYPE_TIMESTAMP="timestamp";
	public static final String TYPE_INT="int";
	public static final String TYPE_DATE="date";
	
	public static final String datetimeFormat="yyyyMMddHHmmssSSS";
	public static final String dateFormat="yyyyMMdd";
	public static final String hiveDatetimeFormat="yyyy-MM-dd HH:mm:ss.SSS";
	public static final SafeSimpleDateFormat sdatetimeFormat = new SafeSimpleDateFormat(datetimeFormat);
	public static final SafeSimpleDateFormat sdateFormat = new SafeSimpleDateFormat(dateFormat);
	
	public static final String HIVE_TYPE_NUMERIC="decimal";
	
	private VarType type;
	private int size;//varchar(size)
	private int precision;//numeric(precision,scale)
	private int scale;
	
	public FieldType(){
	}
	
	public FieldType(VarType type){
		this.type = type;
	}
	
	public FieldType(VarType type, int size){
		this.type = type;
		this.size = size;
	}
	
	public FieldType(VarType type, int precision, int scale){
		this.type = type;
		this.precision = precision;
		this.scale = scale;
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof FieldType)){
			return false;
		}
		FieldType that = (FieldType)obj;
		if (!Objects.equal(that.getType(), type)){
			return false;
		}
		if (size!=that.getSize()){
			return false;
		}
		if (precision!=that.getPrecision()){
			return false;
		}
		if (scale!=that.getScale()){
			return false;
		}
		return true;
	}
	
	public FieldType(String str){
		String[] eles = str.split("[\\(\\),]");
		String t = eles[0].trim();
		if (TYPE_NUMERIC.equals(t)){
			if (eles.length==3){
				this.type = VarType.NUMERIC;
				this.precision = Integer.parseInt(eles[1]);
				this.scale = Integer.parseInt(eles[2]);
			}else{
				logger.error(String.format("numeric type requires two parameters:%s", str));
			}
		}else if (TYPE_VARCHAR.equals(t)){
			if (eles.length==2){
				this.type = VarType.STRING;
				this.size = Integer.parseInt(eles[1]);
			}else{
				logger.error(String.format("string type requires 1 parameter:%s", str));
			}
		}else{
			String[] tn = t.split(" ");
			this.type = VarType.fromValue(tn[0]);
		}
	}
	
	public Object decode(String v){
		if (this.type==VarType.STRING){
			return v;
		}else if (this.type == VarType.TIMESTAMP){
			try {
				return sdatetimeFormat.parse(v);
			}catch(Exception e){
				logger.error(String.format("datetime format expected %s, get %s", datetimeFormat, v));
				return null;
			}
		}else if (this.type == VarType.DATE){
			try{
				return sdateFormat.parse(v);
			}catch(Exception e){
				logger.error(String.format("date format expected %s, get %s", dateFormat, v));
				return null;
			}
		}else if (this.type == VarType.NUMERIC){
			return Double.parseDouble(v);
		}else if (this.type == VarType.INT){
			return Integer.parseInt(v);
		}else{
			logger.error(String.format("type %s not supported for %s", this, v));
			return null;
		}
	}
	
	public VarType getType() {
		return type;
	}
	public void setType(VarType type) {
		this.type = type;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getPrecision() {
		return precision;
	}
	public void setPrecision(int precision) {
		this.precision = precision;
	}
	public int getScale() {
		return scale;
	}
	public void setScale(int scale) {
		this.scale = scale;
	}
	
	public String toString(){
		if (VarType.NUMERIC == this.type){
			return String.format("%s(%d,%d)", TYPE_NUMERIC, precision, scale);
		}else if (VarType.STRING == this.type){
			return String.format("%s(%d)", TYPE_VARCHAR, size);
		}else{
			return this.type.value();
		}
	}
	
	public String toSql(DBType dbtype){
		if (VarType.NUMERIC == this.type){
			if (DBType.VERTICA == dbtype || DBType.NONE == dbtype){
				return String.format("%s(%d,%d)", TYPE_NUMERIC, precision, scale);
			}else if (DBType.HIVE == dbtype){
				return String.format("%s(%d,%d)", HIVE_TYPE_NUMERIC, precision, scale);
			}else{
				logger.error(String.format("dbtype %s not supported for fieldType %s", dbtype, this));
				return null;
			}
		}else if (VarType.STRING == this.type){
			return String.format("%s(%d)", TYPE_VARCHAR, size);
		}else{
			return this.type.value();
		}
	}

}
