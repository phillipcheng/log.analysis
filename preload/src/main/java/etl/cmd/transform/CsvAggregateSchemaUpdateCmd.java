package etl.cmd.transform;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.AggrOperator;
import etl.engine.DynaSchemaFileETLCmd;
import etl.util.DBUtil;
import etl.util.IdxRange;
import etl.util.Util;

public class CsvAggregateSchemaUpdateCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvAggregateSchemaUpdateCmd.class);
	
	public static final String AGGR_OP_SUM="sum";
	public static final String AGGR_OPERATOR_SEP="\\|";
	
	
	public static final String cfgkey_aggr_op="aggr.op";
	public static final String cfgkey_aggr_groupkey="aggr.groupkey";
	
	private AggrOpMap aoMap;
	private List<IdxRange> groupKeys;
	
	public CsvAggregateSchemaUpdateCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		List<Object> strAggrOpList = pc.getList(cfgkey_aggr_op);
		aoMap = new AggrOpMap(strAggrOpList);
		groupKeys = IdxRange.parseString(pc.getString(cfgkey_aggr_groupkey));
	}

	@Override
	public List<String> sgProcess(){
		List<String> attrs = this.logicSchema.getAttrNames(this.oldTable);
		List<String> attrTypes = logicSchema.getAttrTypes(this.oldTable);
		int idxMax = attrs.size()-1;
		List<String> newAttrs = new ArrayList<String>();
		List<String> newTypes = new ArrayList<String>();
		AggrOp aop = new AggrOp(AggrOperator.group, groupKeys);
		aoMap.addAggrOp(aop);
		aoMap.constructMap(idxMax);
		for (int i=0; i<=idxMax; i++){
			if (aoMap.getOp(i)!=null){
				newAttrs.add(attrs.get(i));
				newTypes.add(attrTypes.get(i));
			}
		}
		boolean schemaUpdated = false;
		List<String> createTableSqls = new ArrayList<String>();
		if (!logicSchema.hasTable(newTable)){
			//update schema
			logicSchema.updateTableAttrs(newTable, newAttrs);
			schemaUpdated = true;
			//generate create table
			createTableSqls.add(DBUtil.genCreateTableSql(newAttrs, newTypes, newTable, dbPrefix));
		}else{
			List<String> existNewAttrs = logicSchema.getAttrNames(newTable);
			if (existNewAttrs.equals(newAttrs)){
				//do nothing
			}else{
				//update schema
				logicSchema.updateTableAttrs(newTable, newAttrs);
				schemaUpdated = true;
				//generate alter table
			}
		}
		if (createTableSqls.size()>0){
			//update/create create-table-sql
			String createsqlFileName = String.format("%s%s.%s_%s", schemaHistoryFolder, dbPrefix, DynSchemaCmd.createtablesql_name, wfid);
			Util.appendDfsFile(fs, createsqlFileName, createTableSqls);
			//update/create dyncfg
			
		}
		if (schemaUpdated){
			Util.toDfsJsonFile(fs, this.schemaFile, logicSchema);
		}
		return null;
	}
}