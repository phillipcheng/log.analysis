package etl.cmd.transform;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import etl.engine.DynaSchemaFileETLCmd;
import etl.util.DBUtil;

public class CsvTransSchemaUpdateCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvTransSchemaUpdateCmd.class);

	private static final String cfgkey_add_fields="add.fields";
	
	private List<String> addFieldsNames = new ArrayList<String>();
	private List<String> addFieldsTypes = new ArrayList<String>();
	
	public CsvTransSchemaUpdateCmd(String wfid, String staticCfg, String dynCfg, String defaultFs) {
		super(wfid, staticCfg, dynCfg, defaultFs);
		String[] nvs = pc.getStringArray(cfgkey_add_fields);
		for (String nv:nvs){
			String[] nva = nv.split("\\:");
			addFieldsNames.add(nva[0]);
			addFieldsTypes.add(nva[1]);
		}
	}

	@Override
	public List<String> sgProcess(){
		List<String> attrs = logicSchema.getAttrNames(this.oldTable);
		
		boolean schemaUpdated = false;
		List<String> createTableSqls = new ArrayList<String>();
		List<String> addFns = new ArrayList<String>();
		List<String> addFts = new ArrayList<String>();
		for (int i=0; i<addFieldsNames.size(); i++){
			String fn = addFieldsNames.get(i);
			String ft = addFieldsTypes.get(i);
			if (!attrs.contains(fn)){
				//update schema
				addFns.add(fn);
				addFts.add(ft);
			}
		}
		if (addFns.size()>0){
			logicSchema.addAttributes(oldTable, addFns);
			logicSchema.addAttrTypes(oldTable, addFts);
			schemaUpdated = true;
			createTableSqls.addAll(DBUtil.genUpdateTableSql(addFns, addFts, oldTable, dbPrefix));
		}
		return super.updateDynSchema(createTableSqls, schemaUpdated);
	}
}