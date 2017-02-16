package etl.engine.test;

import etl.util.FieldType;
import etl.util.GroupFun;
import etl.util.NanoTimeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.simple.NanoTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class TestNanoTimeUtils {
    public static final Logger logger = LogManager.getLogger(TestNanoTimeUtils.class);

    @Test
    public void test() throws Exception {
        String datetime = GroupFun.dtStandardize("2016-07-24T00:00:00-05:00", "yyyy-MM-dd'T'HH:mm:ssXXX");
        Date d = FieldType.sdatetimeFormat.parse(datetime);
        NanoTime nt = NanoTimeUtils.getNanoTime(d.getTime(), false);
        logger.info(d);
        long t = NanoTimeUtils.getTimestamp(nt, false);
        logger.info(new Date(t));
        Assert.assertEquals(d.getTime(), t);
    }
}
