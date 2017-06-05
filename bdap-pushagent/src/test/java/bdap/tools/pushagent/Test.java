package bdap.tools.pushagent;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.quartz.Calendar;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.SimpleTriggerImpl;

public class Test implements JobExecutionContext {
	private static final String DEST_SERVER = "DestServer";
	private static final String DEST_SERVER_PORT = "DestServerPort";
	private static final String DEST_SERVER_USER = "DestServerUser";
	private static final String DEST_SERVER_PASS = "DestServerPass";
	private static final String DEST_SERVER_DIR_RULE = "DestServerDirRule";
	private static final String WORKING_ELEMENT = "WorkingElement";
	private static final String WORKING_DIR = "WorkingDir";
	private static final String FILENAME_FILTER = "FilenameFilter";
	private static final String FILES_PER_BATCH = "FilesPerBatch";
	private static final String RECURSIVE = "Recursive";
	private static final String PROCESS_RECORD = "ProcessRecord";
	
	private JobDataMap dataMap;

	public void unused() throws Exception {
		Config c = new Config();
		DirConfig v = new DirConfig();
		v.setId("test");
		v.setDirectory("/tmp/femtocell");
		Element[] elems = new Element[1];
		elems[0] = new Element();
		elems[0].setName("FEMTO");
		elems[0].setHostname("localhost");
		elems[0].setIp("127.0.0.1");
		v.setElements(elems);
		v.setCategory("OM");
		v.setTimeZone("CST");
		v.setCronExpr("");
		v.setRecursive(true);
		v.setFilenameFilterExpr("new('org.apache.commons.io.filefilter.WildcardFileFilter', '*.csv')");
		v.setFilesPerBatch(2);
		v.setDestServer("localhost");
		v.setDestServerDirRule("/tmp/${CURRENT_DATE}/${SRC_IP}");
		v.setDestServerUser("player");
		v.setDestServerPass("123456");
		
		c.put("test", v);
		
		System.out.println(new Date());
		
		Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
		while (nis.hasMoreElements()) {
			NetworkInterface ni = nis.nextElement();
			Enumeration<InetAddress> ias = ni.getInetAddresses();
			System.out.println(ni.getName());
			while (ias.hasMoreElements())
				System.out.println(ias.nextElement().getHostAddress());
		}
	}
	
	@org.junit.Test
	public void test() throws Exception {
		JobExecutionContext context = this;
		dataMap = new JobDataMap();
		Element e = new Element();
		e.setIp("127.0.0.1");
		e.setHostname("localhost");
		e.setName("local");
		dataMap.put(WORKING_ELEMENT, e);
		dataMap.put(WORKING_DIR, "/Dev/log.analysis/bdap-pushagent/src/test/resources");
		dataMap.put(RECURSIVE, true);
		dataMap.put(FILENAME_FILTER, new WildcardFileFilter("*.csv"));
		dataMap.put(FILES_PER_BATCH, 1);
		dataMap.put(PROCESS_RECORD, "/tmp/process_record");
		dataMap.put(DEST_SERVER, "127.0.0.1");
		dataMap.put(DEST_SERVER_PORT, 22);
		dataMap.put(DEST_SERVER_USER, "player");
		dataMap.put(DEST_SERVER_PASS, "123456");
		dataMap.put(DEST_SERVER_DIR_RULE, "'/tmp/test_dir'");
		
		new Main().execute(context);
	}

	public static void main(String[] args) throws Exception {
		Main.main(new String[] {});
		
		long t = System.currentTimeMillis();
		new File("C:/Dev/log.analysis/bdap-pushagent/src/test/resources/z.csv").setLastModified(t);
		new File("C:/Dev/log.analysis/bdap-pushagent/src/test/resources/y.csv").setLastModified(t);
		new File("C:/Dev/log.analysis/bdap-pushagent/src/test/resources/b.csv").setLastModified(t);
		new File("C:/Dev/log.analysis/bdap-pushagent/src/test/resources/c.csv").setLastModified(t);
	}

	@Override
	public Scheduler getScheduler() {
		// TODO Auto-generated method stub
		return null;
	}

	public Trigger getTrigger() {
		return new SimpleTriggerImpl();
	}

	@Override
	public Calendar getCalendar() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRecovering() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public TriggerKey getRecoveringTriggerKey() throws IllegalStateException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRefireCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public JobDataMap getMergedJobDataMap() {
		return dataMap;
	}

	@Override
	public JobDetail getJobDetail() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Job getJobInstance() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getFireTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getScheduledFireTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getPreviousFireTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getNextFireTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFireInstanceId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getResult() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setResult(Object result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getJobRunTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void put(Object key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object get(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

}
