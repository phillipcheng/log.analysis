package etl.cmd.test;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

@InterfaceAudience.Private
public class XLocalClientProtocolProvider extends ClientProtocolProvider {
	private LocalJobRunner jobrunner;
	
	public ClientProtocol create(Configuration conf) throws IOException {
		String framework = conf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
		if (!"xlocal".equals(framework)) {
			return null;
		}
		conf.setInt(JobContext.NUM_MAPS, 1);
		
		if (jobrunner == null)
			jobrunner = new LocalJobRunner(conf);

		return jobrunner;
	}

	public ClientProtocol create(InetSocketAddress addr, Configuration conf) {
		return null; // LocalJobRunner doesn't use a socket
	}

	public void close(ClientProtocol clientProtocol) {
		// no clean up required
	}
}
