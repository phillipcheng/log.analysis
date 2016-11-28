package bdap.schemagen.spi;

import java.io.Reader;

import bdap.schemagen.config.Config;
import etl.engine.LogicSchema;

public interface SchemaGenerator {
	public LogicSchema generate(final Reader reader, Config config) throws Exception;
	public LogicSchema insertSchema(LogicSchema ls, LogicSchema commonLs) throws Exception;
	public LogicSchema outerJoinSchema(LogicSchema ls, LogicSchema additionalLs) throws Exception;
}
