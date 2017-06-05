package bdap.schemagen.test;

import bdap.schemagen.Main;

public class TestMain {

	public static void main(String[] args) throws Exception {
		
		String path = TestCSVSchemaGenerator.class.getResource("/huawei").getPath();
		String dimensionsPath = TestCSVSchemaGenerator.class.getResource("/huawei-dimensions.schema").getPath();
		if (path.startsWith("/C:/"))
			path = path.substring(1);
		if (dimensionsPath.startsWith("/C:/"))
			dimensionsPath = dimensionsPath.substring(1);
		Main.main(new String[] {"-multiple", "-to", path, "-join", path, "-with", dimensionsPath});
	}

}
