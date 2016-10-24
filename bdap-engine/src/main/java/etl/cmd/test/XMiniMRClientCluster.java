package etl.cmd.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRClientCluster;

public class XMiniMRClientCluster implements MiniMRClientCluster {
	private Configuration conf;
	
	public XMiniMRClientCluster(Configuration conf) {
		this.conf = conf;
	}

	public void start() throws IOException {
	}

	public void restart() throws IOException {
	}

	public void stop() throws IOException {
	}

	public Configuration getConfig() throws IOException {
		return conf;
	}

}
