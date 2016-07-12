package hpe.pde.cmd;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.FileETLCmd;
import etl.engine.MRMode;
import etl.engine.ProcessMode;

//key colon value format to csv
public class SesToCsvCmd extends FileETLCmd{
	public static final Logger logger = Logger.getLogger(SesToCsvCmd.class);
	public static final String cfgkey_ses_folder="ses.folder";
	public static final String cfgkey_use_wfid="use.wfid";
	
	private String sesFolder;
	private boolean useWfid;
	
	private String SESSION = "";
	private String ALMAN_EPH_EVENT = "";
	private String ALMAN_EVENT = "";
	private String EPH_EVENT = "";
	private String EPH = "";
	private String ALMAN = "";
	private String END_SESSION = "";
	private String EPH_ALMAN_EVENT = "";


	private String PRIMARY = "";
	private String PRIMARY_INCOMPLETE = "";
	private String INCOMPLETE_RECORD = "";
	private String INCOMPLETE_RECORD_EVENT =  "";
	private String DELTA_INCOMPLETE_RECORD_EVENT = "";
	private static long WEEK_TO_MILLSEC = 7*24*3600*1000;
	private static long gpsElaps = new GregorianCalendar(1980,Calendar.JANUARY,6,0,0,0).getTimeInMillis();
	
	public SesToCsvCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		useWfid = pc.getBoolean(cfgkey_use_wfid, false);
		sesFolder = pc.getString(cfgkey_ses_folder);
		
		SESSION = pc.getString("session");
		ALMAN_EPH_EVENT = pc.getString("alman_eph_event");
		ALMAN_EVENT = pc.getString("alman_event");
		EPH_EVENT = pc.getString("eph_event");
		EPH = pc.getString("eph");
		ALMAN = pc.getString("alman");
		END_SESSION = pc.getString("end_session");
		EPH_ALMAN_EVENT=pc.getString("eph_alman_event");
		PRIMARY = pc.getString("primary_complete");
		PRIMARY_INCOMPLETE = pc.getString("primary_incomplete");
		INCOMPLETE_RECORD = pc.getString("incomplete_record");
		INCOMPLETE_RECORD_EVENT = pc.getString("incomplete_record_event");
		DELTA_INCOMPLETE_RECORD_EVENT = pc.getString("delta_incomplete_record_event");
		this.setMrMode(MRMode.file);
	}
	

	private static String gpsToUTC (long gpsWeek, double gpsSeconds) {
		GregorianCalendar utcC = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		long gpsMilliSec = (long) (gpsSeconds * 1000);
		long utcTime = gpsElaps + (gpsWeek * WEEK_TO_MILLSEC )+ gpsMilliSec;
		utcC.setTimeInMillis(utcTime);        
		SimpleDateFormat formatDate = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.S" );
		String strDate = formatDate.format( utcC.getTime() );
		return strDate;
	}
	
	@Override
	public Map<String, List<String>> mrProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		String filename = row;
		List<String> outputList = new ArrayList<String>();
		Path sesFile = null;
		if (useWfid){
			sesFile = new Path(sesFolder + "/" + wfid + "/" + filename);
		}else{
			sesFile = new Path(sesFolder + "/" + filename);
		}
		BufferedReader br = null;
		try {
			int GPSWEEK = 0;
			int ses_record = 0;
			int end_ses_record = 0;
			int alman_eph_reqres_event_num = 0;
			int alman_reqres_event_num = 0;
			int eph_reqres_event_num = 0;
			int alman_eph_reqres=0;
			int alman_reqres =0;
			int eph_reqres =0 ;
			int COMPLETE_SESSION = 0;
			double SESSION_START_TIME = 0;
			double SESSION_END_TIME = 0;
			double DURATION = 0;
			double DELTA_TIME = 0;
			boolean alm_eph_chk = false;
			boolean Incomplete_Session_StartRecord = false;
			boolean Delta_Session_StartRecord = false;
			String START_TIME = null;
			String END_TIME = null;
			String SESSION_ID = null;
			String WEEK = null;
			String DELTA = null;
			String Session_StartTime = null;
			String Incomplete_Session_Week = null;
			String Incomplete_Session_StartTime = null;
			String Incomplete_Session_EndTime = null;
			String Duration = null;
			String Primary_Incomplete_Session_StartTime = null;
			br=new BufferedReader(new InputStreamReader(fs.open(sesFile)));
			for(String line; (line = br.readLine()) != null; ) {
				if (line.matches("")) {
					continue;
				}
				if (COMPLETE_SESSION == 1) {
					if ( (alm_eph_chk == true) && (line.matches(END_SESSION) == false) ) {
						continue;
					}
					else if (line.matches(ALMAN_EPH_EVENT)||line.matches(EPH_ALMAN_EVENT)) {
						alman_eph_reqres_event_num = 1;
						alman_eph_reqres  = 1;
						alm_eph_chk = true;
						continue;
					}
					else if (line.matches(EPH_EVENT)&&line.matches(EPH)) {
						eph_reqres_event_num = 1;
						eph_reqres  = 1;
						continue;
					}
					else if (line.matches(ALMAN_EVENT)&&line.matches(ALMAN)) {
						alman_reqres_event_num = 1;
						alman_reqres  = 1;
						continue;
					}
				}
				if (COMPLETE_SESSION == 2) {
					if (line.matches(INCOMPLETE_RECORD_EVENT) && line.matches(ALMAN_EVENT) && line.matches(EPH_EVENT) && (Incomplete_Session_StartRecord == true)) {
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_EndTime = tokens[2].toString();
						continue;
					}
					else if (line.matches(INCOMPLETE_RECORD_EVENT) && line.matches(ALMAN_EVENT) && line.matches(EPH_EVENT) && (Incomplete_Session_StartRecord == false)) {
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_Week = tokens[1].toString();
						Incomplete_Session_StartTime = tokens[2].toString();
						Incomplete_Session_StartRecord = true;
						continue;
					}
					else if ( (line.matches(ALMAN_EPH_EVENT)||line.matches(EPH_ALMAN_EVENT)) && line.matches(INCOMPLETE_RECORD_EVENT) && (Incomplete_Session_StartRecord == true)) {
						alman_eph_reqres_event_num = 1;
						alman_eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_EndTime = tokens[2].toString();
						continue;
					}
					else if ( (line.matches(ALMAN_EPH_EVENT)||line.matches(EPH_ALMAN_EVENT)) && line.matches(INCOMPLETE_RECORD_EVENT) && (Incomplete_Session_StartRecord == false)) {
						alman_eph_reqres_event_num = 1;
						alman_eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_Week = tokens[1].toString();
						Incomplete_Session_StartTime = tokens[2].toString();
						Incomplete_Session_StartRecord = true;
						continue;
					}
					else if ( line.matches(EPH_EVENT) && line.matches(EPH) && line.matches(INCOMPLETE_RECORD_EVENT) && (Incomplete_Session_StartRecord == true) ) {
						eph_reqres_event_num = 1;
						eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_EndTime = tokens[2].toString();
						continue;
					}
					else if (line.matches(EPH_EVENT) && line.matches(EPH) && line.matches(INCOMPLETE_RECORD_EVENT) && (Incomplete_Session_StartRecord == false)) {
						eph_reqres_event_num = 1;
						eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_Week = tokens[1].toString();
						Incomplete_Session_StartTime = tokens[2].toString();
						Incomplete_Session_StartRecord = true;
						continue;
					}
					else if (line.matches(ALMAN_EVENT)&&line.matches(ALMAN) && line.matches(INCOMPLETE_RECORD_EVENT) && (Incomplete_Session_StartRecord == true)) {
						alman_reqres_event_num = 1;
						alman_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_EndTime = tokens[2].toString();
						continue;
					}
					else if (line.matches(ALMAN_EVENT)&&line.matches(ALMAN) && line.matches(INCOMPLETE_RECORD_EVENT) && (Incomplete_Session_StartRecord == false) ) {
						alman_reqres_event_num = 1;
						alman_reqres  = 1; 
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						Incomplete_Session_Week = tokens[1].toString();
						Incomplete_Session_StartTime = tokens[2].toString();
						Incomplete_Session_StartRecord = true;
						continue;
					}
				}
				if ( COMPLETE_SESSION == 3 ) {
					if (line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && line.matches(ALMAN_EVENT) && line.matches(EPH_EVENT) && (Delta_Session_StartRecord == true)) {
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						continue;
					}
					else if (line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && line.matches(ALMAN_EVENT) && line.matches(EPH_EVENT) && (Delta_Session_StartRecord == false)) {
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						Delta_Session_StartRecord = true;
						continue;
					}
					else if ( (line.matches(ALMAN_EPH_EVENT)||line.matches(EPH_ALMAN_EVENT)) && line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && (Delta_Session_StartRecord == true)) {
						alman_eph_reqres_event_num = 1;
						alman_eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						continue;
					}
					else if ( (line.matches(ALMAN_EPH_EVENT)||line.matches(EPH_ALMAN_EVENT)) && line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && (Delta_Session_StartRecord == false)) {
						alman_eph_reqres_event_num = 1;
						alman_eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						Delta_Session_StartRecord = true;
						continue;
					}
					else if ( line.matches(EPH_EVENT) && line.matches(EPH) && line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && (Delta_Session_StartRecord == true) ) {
						eph_reqres_event_num = 1;
						eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						continue;
					}
					else if (line.matches(EPH_EVENT) && line.matches(EPH) && line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && (Delta_Session_StartRecord == false)) {
						eph_reqres_event_num = 1;
						eph_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						Delta_Session_StartRecord = true;
						continue;
					}
					else if (line.matches(ALMAN_EVENT)&&line.matches(ALMAN) && line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && (Delta_Session_StartRecord == true)) {
						alman_reqres_event_num = 1;
						alman_reqres  = 1;
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						continue;
					}
					else if (line.matches(ALMAN_EVENT)&&line.matches(ALMAN) && line.matches(DELTA_INCOMPLETE_RECORD_EVENT) && (Delta_Session_StartRecord == false) ) {
						alman_reqres_event_num = 1;
						alman_reqres  = 1; 
						String line2=line.replaceAll("\\s+", "|");
						String[] tokens = line2.split("\\|");
						DELTA = tokens[1].toString();
						Delta_Session_StartRecord = true;
						continue;
					}
				}
				if (line.matches(SESSION)) {
					ses_record = 1;
					String line2=line.replaceAll("\\s+","|"); 	
					String[] tokens = line2.split("\\|");  
					SESSION_ID = tokens[1].toString();
					continue;
				}	
				else if ( line.matches(PRIMARY)) {
					String line2=line.replaceAll("\\s+","|");
					String[] tokens = line2.split("\\|");
					WEEK = tokens[6].toString();
					Session_StartTime = tokens[8].toString();
					Duration = tokens[10].toString();
					COMPLETE_SESSION = 1;
					continue;
				}
				else if ( line.matches(INCOMPLETE_RECORD)) {
					COMPLETE_SESSION = 2;
					continue;
				}
				else if (line.matches(PRIMARY_INCOMPLETE)) {
					String line2=line.replaceAll("\\s+","|");
					String[] tokens = line2.split("\\|");
					WEEK = tokens[6].toString();
					Primary_Incomplete_Session_StartTime = tokens[8].toString();
					COMPLETE_SESSION = 3;
					continue;
				}
				else if (line.matches(END_SESSION)) {
					end_ses_record = 1;
				}	

				if ( end_ses_record == 1 ) {
					if ( COMPLETE_SESSION == 1 ) {
						GPSWEEK = Integer.parseInt(WEEK);
						SESSION_START_TIME = Double.parseDouble(Session_StartTime);
						if ( Duration.matches("\\d+.\\d++")) {
							DURATION = Double.parseDouble(Duration);
						}
						else {
							DURATION = 0;
						}
						SESSION_END_TIME = SESSION_START_TIME + DURATION;
						START_TIME = gpsToUTC ( GPSWEEK, SESSION_START_TIME );
						END_TIME = gpsToUTC ( GPSWEEK, SESSION_END_TIME );
					}
					if ( COMPLETE_SESSION == 2 ) {
						GPSWEEK = Integer.parseInt(Incomplete_Session_Week);
						SESSION_START_TIME = Double.parseDouble(Incomplete_Session_StartTime);
						SESSION_END_TIME = Double.parseDouble(Incomplete_Session_EndTime);
						START_TIME = gpsToUTC (GPSWEEK, SESSION_START_TIME );
						END_TIME = gpsToUTC (GPSWEEK, SESSION_END_TIME);
						DURATION = SESSION_END_TIME - SESSION_START_TIME;
					}
					if ( COMPLETE_SESSION == 3 ) {
						GPSWEEK = Integer.parseInt(WEEK);
						SESSION_START_TIME = Double.parseDouble(Primary_Incomplete_Session_StartTime);
						DELTA_TIME = Double.parseDouble(DELTA);
						SESSION_END_TIME = SESSION_START_TIME + DELTA_TIME;
						START_TIME = gpsToUTC (GPSWEEK, SESSION_START_TIME );
						END_TIME = gpsToUTC (GPSWEEK, SESSION_END_TIME);
						DURATION = SESSION_END_TIME - SESSION_START_TIME;
					}
					String lineOutput = null;
					if ( ses_record == 1 && eph_reqres_event_num == 0 && alman_eph_reqres_event_num == 0 && alman_reqres_event_num == 0  ) {
						lineOutput = String.format("%s,%s,%s,%s,%d,%d",SESSION_ID,START_TIME,END_TIME,DURATION,0,0);
					}
					else if ( ses_record == 1 && alman_eph_reqres_event_num == 1 ) {
						lineOutput = String.format("%s,%s,%s,%s,%d,%d",SESSION_ID,START_TIME,END_TIME,DURATION,alman_eph_reqres,1);
					}
					else if ( ses_record == 1 && eph_reqres_event_num == 1 ) {
						lineOutput = String.format("%s,%s,%s,%s,%d,%d",SESSION_ID,START_TIME,END_TIME,DURATION,0,eph_reqres);
					}
					else if ( ses_record == 1 && alman_reqres_event_num == 1 ) {
						lineOutput = String.format("%s,%s,%s,%s,%d,%d",SESSION_ID,START_TIME,END_TIME,DURATION,alman_reqres,0);
					}else{
						logger.warn(String.format("no output for line:%s", line));
					}
					if (lineOutput!=null){
						if (isAddFileName() && context!=null){
							lineOutput+="," + getAbbreFileName(filename);
						}
						outputList.add(lineOutput);
					}

					ses_record = 0;
					alman_eph_reqres_event_num = 0;
					alman_eph_reqres  = 0;
					eph_reqres_event_num = 0;
					eph_reqres  = 0;
					alman_reqres_event_num = 0;
					alman_reqres  = 0;
					end_ses_record = 0;

					alm_eph_chk = false;
					COMPLETE_SESSION = 0;
					Incomplete_Session_StartRecord = false;
					Delta_Session_StartRecord = false;
					GPSWEEK = 0;
					SESSION_START_TIME = 0;
					SESSION_END_TIME = 0;
					DURATION = 0;
					DELTA_TIME = 0;
					START_TIME = null;
					END_TIME = null;

					WEEK = null;
					DELTA = null;
					Session_StartTime = null;
					Incomplete_Session_Week = null;
					Primary_Incomplete_Session_StartTime = null;
					Incomplete_Session_StartTime = null;
					Incomplete_Session_EndTime = null;
					Duration = null;
					SESSION_ID = null; 
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (br!= null){
				try{
					br.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		Map<String, List<String>> retMap = new HashMap<String, List<String>>();
		retMap.put(RESULT_KEY_OUTPUT, outputList);
		List<String> infoList = new ArrayList<String>();
		infoList.add(outputList.size()+"");
		retMap.put(RESULT_KEY_LOG, infoList);
		return retMap;
	}
}
