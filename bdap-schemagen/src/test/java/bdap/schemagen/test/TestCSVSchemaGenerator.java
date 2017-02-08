package bdap.schemagen.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import bdap.schemagen.CSVSchemaGenerator;
import bdap.schemagen.config.Config;
import bdap.schemagen.config.FieldConfig;
import bdap.schemagen.config.ItemConfig;
import bdap.util.JsonUtil;
import etl.engine.LogicSchema;
import etl.engine.types.DBType;
import etl.util.SchemaUtils;
import etl.util.StoreFormat;
import etl.util.VarType;

public class TestCSVSchemaGenerator {

	public static void main(String[] args) throws Exception {
		InputStream in;
		BufferedReader reader;
		
		in = TestCSVSchemaGenerator.class.getResourceAsStream("/femto/sFemto_OMRT_20150923.csv");
		reader = new BufferedReader(new InputStreamReader(in, "utf8"));

		try {
			CSVSchemaGenerator g = new CSVSchemaGenerator();

			Config config = new Config();
			ItemConfig tableId = new ItemConfig();
			ItemConfig tableName = new ItemConfig();
			tableId.setHeaderColumnName("OM Keys");
			tableId.setComparisonMethod("startsWith");
			tableName.setHeaderColumnName("OM Groups");
			config.setTableId(tableId);
			config.setTableName(tableName);
			FieldConfig fc1 = new FieldConfig();
			FieldConfig fc2 = new FieldConfig();
			fc1.setHeaderColumnName("OM Registers");
			fc1.setFieldTypeHeaderColumnName("OM Types");
			fc1.setFieldSizeHeaderColumnName("OM Size");
			fc2.setHeaderColumnName("OM Indexes");
			fc2.setMultiple(true);
			fc2.setMultipleSeparator(",");
			fc2.setDefaultFieldType(VarType.STRING.value());
			config.setFields(new FieldConfig[] { fc1, fc2 });
			
			JsonUtil.toLocalJsonFile("config.json", config);

			LogicSchema ls = g.generate(reader, config);
			System.out.println(ls);
			
			String path = TestCSVSchemaGenerator.class.getResource("/femto/common-schema.json").getPath();
			if (path.startsWith("/C:/"))
				path = path.substring(1);
			LogicSchema commonLs = SchemaUtils.fromLocalJsonPath(path, LogicSchema.class);
			
			g.insertSchema(ls, commonLs);
			
			JsonUtil.toLocalJsonFile("test.ls", ls);
			
			List<String> ddl = SchemaUtils.genCreateSqlByLogicSchema(ls, "test", DBType.VERTICA, StoreFormat.text);
			if (ddl != null && ddl.size() > 0) {
				BufferedWriter writer = new BufferedWriter(new FileWriter("test.ddl"));
				for (String line: ddl) {
					writer.write(line);
					writer.write(';');
					writer.newLine();
				}
	            writer.close();
			}

		} finally {
			reader.close();
			in.close();
		}

	}

}
