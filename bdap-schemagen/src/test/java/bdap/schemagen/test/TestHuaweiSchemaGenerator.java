package bdap.schemagen.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import bdap.schemagen.CSVSchemaGenerator;
import bdap.schemagen.config.Config;
import bdap.util.JsonUtil;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.SchemaUtils;

public class TestHuaweiSchemaGenerator {

	public static void main(String[] args) throws Exception {

		InputStream in = TestHuaweiSchemaGenerator.class.getResourceAsStream("/huawei/common-schema.json");
		BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf8"));
		StringBuilder strbuf = new StringBuilder();
		String str;
		while ((str = reader.readLine()) != null) {
			strbuf.append(str);
		}
		reader.close();
		in.close();

		try {
			CSVSchemaGenerator g = new CSVSchemaGenerator();
			
			/* Generate the measures schema */
			String path = TestHuaweiSchemaGenerator.class.getResource("/huawei/config.json").getPath();
			if (path.startsWith("/C:/"))
				path = path.substring(1);
			Config config = (Config) JsonUtil.fromLocalJsonFile(path, Config.class);

			in = TestHuaweiSchemaGenerator.class.getResourceAsStream("/huawei/RAN Huawei CounterID to Name Translation Table.csv");
			reader = new BufferedReader(new InputStreamReader(in, "utf8"));

			LogicSchema ls = g.generate(reader, config);
			System.out.println(ls);
			
			reader.close();
			in.close();
			
			LogicSchema commonLs = (LogicSchema) JsonUtil.fromJsonString(strbuf.toString(), LogicSchema.class);
			
			g.insertSchema(ls, commonLs);
			
			/* Generate the dimensions schema */
			path = TestHuaweiSchemaGenerator.class.getResource("/huawei/dimensions-config.json").getPath();
			if (path.startsWith("/C:/"))
				path = path.substring(1);
			config = (Config) JsonUtil.fromLocalJsonFile(path, Config.class);
			
			in = TestHuaweiSchemaGenerator.class.getResourceAsStream("/huawei/TableDimensions.csv");
			reader = new BufferedReader(new InputStreamReader(in, "utf8"));
			
			LogicSchema ls2 = g.generate(reader, config);
			
			ls = g.joinSchema(ls, ls2);
			
			JsonUtil.toLocalJsonFile("huawei.schema", ls);
			
			List<String> ddl = SchemaUtils.genCreateSqlByLogicSchema(ls, "mydb", DBType.VERTICA);
			if (ddl != null && ddl.size() > 0) {
				BufferedWriter writer = new BufferedWriter(new FileWriter("huawei.ddl"));
				for (String line: ddl) {
					writer.write(line);
					writer.write(';');
					writer.newLine();
				}
	            writer.close();
			}
			
			System.out.println(ls);

		} finally {
			reader.close();
			in.close();
		}

	}

}
