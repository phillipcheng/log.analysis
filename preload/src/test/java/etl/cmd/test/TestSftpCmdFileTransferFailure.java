package etl.cmd.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Vector;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class TestSftpCmdFileTransferFailure {
	private int sftpRetryCount;
	private int sftpConnectRetryCount;
	private String fromDir = "/data/mtccore/source/";
	@Test
	public void test1() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {/*
	    	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			String localFolder = "C:\\Users\\rangasap\\workspace\\log.analysis\\preload\\src\\test\\resources\\";
	    	String dfsFolder = "/mtccore/etlcfg/";
	    	String cfg = "sftp.properties";
			fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process("sftp.host=192.85.247.104, sftp.folder=/data/log.analysis/bin/", null);
			return null;
	      */

	  		Session session = null;
	  		ChannelSftp sftpChannel = null;
	  		int retryCntTemp = 1;
	  		int sftConnectRetryCntTemp = 1;
	  		OutputStream fsos = null;
	  		InputStream is = null;
	  		

	  		Configuration conf = new Configuration();
	  		String defaultFS = "hdfs://192.85.247.104:19000";
	  		conf.set("fs.defaultFS", defaultFS);
	  		FileSystem fs = FileSystem.get(conf);
	  		
	  		try {
	  			//override param
	  	
	  			
	  			////added for sftp retry
	  			//conversionException is already handled here.
	  			sftpRetryCount = 3;
	  			sftpConnectRetryCount = 3;
	  			//connect
	  			
	  			try {
	  					JSch jsch = new JSch();
	  				    Channel channel = null;
	  				    session = jsch.getSession("dbadmin", "192.85.247.104", 22);
	  				    if(session == null){
	  				    	System.out.println("Session is not created.");
	  				    }
	  				    session.setConfig("StrictHostKeyChecking", "no");
	  				    session.setPassword("password");
	  				    //retry for connecting to session
	  				    while(sftConnectRetryCntTemp <= sftpConnectRetryCount){
	  				    	session.connect();
	  				    	if(session.isConnected()){
	  		  				    System.out.println("Session Connected");
	  				        	channel = session.openChannel("sftp");
	  				        	break;
	  				        }else{
	  				        	if(sftConnectRetryCntTemp == sftpConnectRetryCount){
	  				        		System.out.println("Reached maximum number of times to make the connection with session.");
	  				        		break;
	  				        	}
	  				        	System.out.println("Session is not connected.hence retrying...");
	  				        	Thread.sleep(10000L);
	  				        	sftConnectRetryCntTemp++;
	  				        }
	  				    }
	  				    
	  				    
	  				    channel.connect();
	  				    System.out.println("Channel Connected");
	  				    sftpChannel = (ChannelSftp) channel;
	  				    System.out.println("From Dir:"+fromDir);
	  				    try {
							sftpChannel.cd(fromDir);
						} catch (Exception e2) {
							// TODO Auto-generated catch block
							System.out.println("Directory does not exist.");
							e2.printStackTrace();
						}
	  				    Vector<LsEntry> v = sftpChannel.ls("*");
	  				    for (LsEntry entry:v){
	  				    	retryCntTemp = 1;
	  				    	String srcFile = fromDir + entry.getFilename();
	  				    	String destFile = "/mtccore/xmldata/" + entry.getFilename();
	  				    	System.out.println(String.format("get file from %s to %s", srcFile, destFile));
	  				    	//OutputStream fsos = new FileOutputStream(new File(destFile));
	  				    	while(retryCntTemp <= sftpRetryCount){
	  					    	try {
	  								fsos = fs.create(new Path(destFile));
	  								is = sftpChannel.get(srcFile);
	  								IOUtils.copy(is, fsos);
	  								  if(fs.exists(new Path(destFile))&&false){
	  									    System.out.println(" transferring file:"+srcFile+" to the destination:"+destFile+ " is completed");
	  										break;	  								
	  									}else{
	  										System.out.println(" transferring file:"+srcFile+" to the destination:"+destFile+ "failed hence retrying..");
	  										throw new Exception("File transfer failed");
	  									}
	  							} catch (Exception e) {
	  								// TODO Auto-generated catch block
	  								e.printStackTrace();
	  								System.out.println("Exception during transferring the file.");
	  					            if (retryCntTemp == sftpRetryCount) {
	  					            	System.out.println("Problem persists while transferring file:"+srcFile+" to the destination:"+destFile+" Exception Message"+ e.getMessage()+" Retried for maximum times. exiting sftp download.");
	  					            	break;
	  					            }else{
	  					            	retryCntTemp++;
	  					            	System.out.println("Problem persists while transferring file:"+srcFile+" to the destination:"+destFile+" Exception Message"+ e.getMessage()+ " Retrying..." + "Retrying count: "+ retryCntTemp);
	  						            try {
	  										Thread.sleep(10000L);
	  									} catch (InterruptedException e1) {
	  										System.out.println("Interripted exception during wait.");
	  										e1.printStackTrace();
	  										continue;
	  									}
	  								}
	  							}				
	  					    	}
	  				    	
	  						if(fsos != null){
	  							fsos.close();
	  						}
	  						if(is != null){
	  							is.close();
	  						}
	  						
	  				    	//deleting file one by one if sftp.clean is enabled
	  				    	if(false){
	  				    		//System.out.println("Deleting file:"+srcFile);
	  				    		sftpChannel.rm(srcFile);
	  				    	}
	  				    }
	  				    //deleting directory
	  				    //sftpChannel.rmdir(fromDir);
	  				
	  			} catch (Exception e) {
	  				System.out.println("Exception during SFTP process.");
	  			}
	  			
	  		}catch(Exception e){
	  			System.out.println("Exception while processing SFTP:");
	  		}finally{
	  			try {
	  				if(fsos != null){
	  					fsos.close();
	  				}
	  				if(is != null){
	  					is.close();
	  				}
	  				if (sftpChannel!=null){
	  					sftpChannel.exit();
	  				}
	  				if (session!=null){
	  					session.disconnect();
	  				}
	  			} catch (IOException e) {
	  				// TODO Auto-generated catch block
	  				System.out.println("Error while executing finally block.");
	  				e.printStackTrace();
	  			}
	  		}
	  		return null;
	  	}
	    });
	}
}

