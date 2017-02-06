package etl.cmd.transform;

import java.io.Serializable;
import java.util.List;

import etl.util.IdxRange;

public class TableIdx implements Serializable{
	
	private String tableName;
	private List<IdxRange> idxR;
	private int colNum;
	
	public TableIdx(String n, List<IdxRange> i){
		tableName = n;
		idxR = i;
	}
	public String toString(){
		return String.format("%s,%d,%s", tableName,colNum, idxR);
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public List<IdxRange> getIdxR() {
		return idxR;
	}
	public void setIdxR(List<IdxRange> idxR) {
		this.idxR = idxR;
	}
	public int getColNum() {
		return colNum;
	}
	public void setColNum(int colNum) {
		this.colNum = colNum;
	}
}
