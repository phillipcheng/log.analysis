package hpe.mtccore.test;

import java.io.Serializable;
import org.apache.log4j.Logger;
import org.junit.Test;

import mtccore.sgsiwf.SGSIWFFlow;

public class SparkAppDriver implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = Logger.getLogger(SparkAppDriver.class);
	
	@Test
	public void sgsiwfStreaming(){
		SGSIWFFlow.main(null);
	}
}
