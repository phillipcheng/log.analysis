package etl.flow.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import etl.cmd.SftpCmd;
import etl.flow.ActionNode;
import etl.flow.Data;
import etl.flow.EndNode;
import etl.flow.ExeType;
import etl.flow.Flow;
import etl.flow.Link;
import etl.flow.LinkType;
import etl.flow.Node;
import etl.flow.StartNode;
import etl.flow.oozie.OozieGenerator;
import etl.flow.oozie.wf.WORKFLOWAPP;
import etl.util.JaxbUtil;

public class FlowGenTest {

	private Flow genPdeFlow(){
		Flow flow = new Flow("pde");
		String defaultFs = "hdfs://127.0.0.1:19000/";
		String sftpServerHost = "192.85.247.104";
		String sftpUser="dbadmin";
		String sftpPass = "password";
		String sftpServerDir="/data/pde/raw";
		
		Map<String, String> wfProperties = new HashMap<String, String>();
		flow.setProperties(wfProperties);
		wfProperties.put(Flow.key_defaultFs, defaultFs);
		wfProperties.put(Flow.key_wfName, "pde");
		
		StartNode start = new StartNode(5*60, 0, 1);
		flow.setStart(start);
		EndNode end = new EndNode(2,0);
		flow.setEnd(end);
		
		//action nodes
		Map<String, Node> actionNodes = new HashMap<String, Node>(); 
		{
			actionNodes.put(start.getName(), start);
			actionNodes.put(end.getName(), end);
			{
				//sftp action
				ActionNode sftp = new ActionNode("sftp", ExeType.java, 0, 1);
				Map<String, String> sftpProperties = new HashMap<String, String>();
				sftpProperties.put(ActionNode.key_cmd_class, "etl.cmd.SftpCmd");
				sftpProperties.put(SftpCmd.cfgkey_incoming_folder, "'/mtccore/xmldata/' + WFID + '/'");
				sftpProperties.put(SftpCmd.cfgkey_sftp_clean, "false");
				sftpProperties.put(SftpCmd.cfgkey_sftp_host, sftpServerHost);
				sftpProperties.put(SftpCmd.cfgkey_sftp_port, "22");
				sftpProperties.put(SftpCmd.cfgkey_sftp_user, sftpUser);
				sftpProperties.put(SftpCmd.cfgkey_sftp_pass, sftpPass);
				sftpProperties.put(SftpCmd.cfgkey_sftp_folder, sftpServerDir);
				sftp.setProperties(sftpProperties);
				actionNodes.put(sftp.getName(), sftp);
			}
			{
				//traceFilter action
				ActionNode traceFilter = new ActionNode("traceFilter", ExeType.mr, 1, 3);
				Map<String, String> traceFilterProperties= new HashMap<String, String>();
				traceFilterProperties.put(ActionNode.key_cmd_class, "etl.cmd.ShellCmd");
				traceFilter.setProperties(traceFilterProperties);
				actionNodes.put(traceFilter.getName(), traceFilter);
			}
			{
				//ses2csv action
				ActionNode ses2csv = new ActionNode("ses2csv", ExeType.mr, 1, 1);
				Map<String, String> ses2csvProperties= new HashMap<String, String>();
				ses2csvProperties.put(ActionNode.key_cmd_class, "hpe.pde.cmd.SesToCsvCmd");
				ses2csv.setProperties(ses2csvProperties);
				actionNodes.put(ses2csv.getName(), ses2csv);
			}
			{
				//csvTrans action
				ActionNode csvTrans = new ActionNode("csvTrans", ExeType.mr, 1, 1);
				Map<String, String> csvTransProperties= new HashMap<String, String>();
				csvTransProperties.put(ActionNode.key_cmd_class, "etl.cmd.CsvTransformCmd");
				csvTrans.setProperties(csvTransProperties);
				actionNodes.put(csvTrans.getName(), csvTrans);
			}
			{
				//fix2csv action
				ActionNode fix2csv = new ActionNode("fix2csv", ExeType.mr, 1, 1);
				Map<String, String> fix2csvProperties= new HashMap<String, String>();
				fix2csvProperties.put(ActionNode.key_cmd_class, "etl.cmd.KcvToCsvCmd");
				fix2csv.setProperties(fix2csvProperties);
				actionNodes.put(fix2csv.getName(), fix2csv);
			}
			{
				//merge action
				ActionNode merge = new ActionNode("merge", ExeType.mr, 2, 1);
				Map<String, String> mergeProperties= new HashMap<String, String>();
				mergeProperties.put(ActionNode.key_cmd_class, "etl.cmd.CsvMergeCmd");
				merge.setProperties(mergeProperties);
				actionNodes.put(merge.getName(), merge);
			}
			{
				//load ses
				ActionNode loadSes = new ActionNode("loadSes", ExeType.java, 1, 1);
				Map<String, String> loadSesProperties= new HashMap<String, String>();
				loadSesProperties.put(ActionNode.key_cmd_class, "etl.cmd.LoadDataCmd");
				loadSes.setProperties(loadSesProperties);
				actionNodes.put(loadSes.getName(), loadSes);
			}
			{
				//load merged
				ActionNode loadMerged = new ActionNode("loadMerged", ExeType.java, 1, 1);
				Map<String, String> loadMergedProperties= new HashMap<String, String>();
				loadMergedProperties.put(ActionNode.key_cmd_class, "etl.cmd.LoadDataCmd");
				loadMerged.setProperties(loadMergedProperties);
				actionNodes.put(loadMerged.getName(), loadMerged);
			}
		}
		flow.setNodes(actionNodes);
		//data
		Map<String, Data> dataMap = new HashMap<String, Data>();
		{
			Data bin = new Data("bin", "defaultFs + '/pde/bin/' + WFID");
			dataMap.put(bin.getName(), bin);
			Data csv = new Data("csv", "defaultFs + '/pde/csv/' + WFID");
			dataMap.put(csv.getName(), csv);
			Data fix = new Data("fix", "defaultFs + '/pde/fix/' + WFID");
			dataMap.put(fix.getName(), fix);
			Data ses = new Data("ses", "defaultFs + '/pde/ses/' + WFID");
			dataMap.put(ses.getName(), ses);
			Data transcsv = new Data("transcsv", "defaultFs + '/pde/transcsv/' + WFID");
			dataMap.put(transcsv.getName(), transcsv);
			Data fixcsv = new Data("fix", "defaultFs + '/pde/fixcsv/' + WFID");
			dataMap.put(fixcsv.getName(), fixcsv);
			Data sescsv = new Data("ses", "defaultFs + '/pde/sescsv/' + WFID");
			dataMap.put(sescsv.getName(), sescsv);
			Data mergecsv = new Data("mergecsv", "defaultFs + '/pde/mergecsv/' + WFID");
			dataMap.put(mergecsv.getName(), mergecsv);
		}
		flow.setDataMap(dataMap);
		
		//links
		List<Link> links = new ArrayList<Link>();
		{
			Link link = new Link(StartNode.start_node_name, "sftp");
			links.add(link);
			
			link = new Link("sftp", "traceFilter", LinkType.success, "bin", 0, 0);
			links.add(link);
			
			link = new Link("traceFilter", "ses2csv", LinkType.success, "ses", 0, 0);
			links.add(link);
			
			link = new Link("traceFilter", "csvTrans", LinkType.success, "csv", 1, 0);
			links.add(link);
			
			link = new Link("traceFilter", "fix2csv", LinkType.success, "fix", 2, 0);
			links.add(link);
			
			link = new Link("ses2csv", "loadSes", LinkType.success, "ses2csv", 0, 0);
			links.add(link);
			
			link = new Link("csvTrans", "merge", LinkType.success, "transcsv", 0, 0);
			links.add(link);
			
			link = new Link("fix2csv", "merge", LinkType.success, "fixcsv", 0, 1);
			links.add(link);
			
			link = new Link("merge", "loadMerged", LinkType.success, "mergecsv", 0, 0);
			links.add(link);
			
			link = new Link("loadSes", EndNode.end_node_name);
			links.add(link);
			
			link = new Link("loadMerged", EndNode.end_node_name);
			links.add(link);
		}
		flow.setLinks(links);
		
		return flow;
	}
	
	private Flow genSgsiwfFlow(){
		Flow flow = new Flow("sgsiwf");

		String defaultFs = "hdfs://127.0.0.1:19000/";
		String sftpServerHost = "192.85.247.104";
		String sftpUser="dbadmin";
		String sftpPass = "password";
		String sftpServerDir="/data/mtccore/raw";
		
		Map<String, String> wfProperties = new HashMap<String, String>();
		wfProperties.put(Flow.key_defaultFs, defaultFs);
		
		StartNode start = new StartNode(5*60, 0, 1);
		flow.setStart(start);
		EndNode end = new EndNode(1, 0);
		flow.setEnd(end);
		
		//action nodes
		Map<String, Node> actionNodes = new HashMap<String, Node>(); 
		actionNodes.put(start.getName(), start);
		actionNodes.put(end.getName(), end);
		{	
			ActionNode sftp = new ActionNode("sftp", ExeType.java, 0, 1);
			Map<String, String> sftpProperties = new HashMap<String, String>();
			sftpProperties.put(ActionNode.key_cmd_class, "etl.cmd.SftpCmd");
			sftpProperties.put(SftpCmd.cfgkey_incoming_folder, "'/mtccore/xmldata/' + WFID + '/'");
			sftpProperties.put(SftpCmd.cfgkey_sftp_clean, "false");
			sftpProperties.put(SftpCmd.cfgkey_sftp_host, sftpServerHost);
			sftpProperties.put(SftpCmd.cfgkey_sftp_port, "22");
			sftpProperties.put(SftpCmd.cfgkey_sftp_user, sftpUser);
			sftpProperties.put(SftpCmd.cfgkey_sftp_pass, sftpPass);
			sftpProperties.put(SftpCmd.cfgkey_sftp_folder, sftpServerDir);
			//add other properties
			sftp.setProperties(sftpProperties);
			actionNodes.put(sftp.getName(), sftp);
		}
		{
			ActionNode genCsv = new ActionNode("genCsv", ExeType.mr, 1, 1);
			Map<String, String> genCsvProperties = new HashMap<String, String>();
			genCsvProperties.put(ActionNode.key_cmd_class, "etl.cmd.XmlToCsvCmd");
			
			//add other properties
			genCsv.setProperties(genCsvProperties);
			actionNodes.put(genCsv.getName(), genCsv);
		}
		flow.setNodes(actionNodes);
		//data
		Map<String, Data> dataMap = new HashMap<String, Data>();
		{
			Data xmlData = new Data("xml", "defaultFs + '/mtccore/xmldata/' + WFID");
			dataMap.put(xmlData.getName(), xmlData);
		}
		flow.setDataMap(dataMap);
		
		//links
		List<Link> links = new ArrayList<Link>();
		{
			Link link = new Link(StartNode.start_node_name, "sftp");
			links.add(link);
			
			link = new Link("sftp", "genCsv", LinkType.success, "xml", 0, 0);
			links.add(link);
			
			link = new Link("genCsv", EndNode.end_node_name);
			links.add(link);
		}
		flow.setLinks(links);
		
		return flow;
	}
	
	@Test
	public void testGenSgsiwfSpark(){
		
	}
	
	@Test
	public void testGenSgsiwfOozie(){
		
	}
	
	@Test
	public void testGenPdeOozie(){
		Flow flow = genPdeFlow();
		WORKFLOWAPP wfa = OozieGenerator.genWfXml(flow);
		String pdeflow = "pde.gen.workflow.xml";
		JaxbUtil.marshal(wfa, pdeflow);
	}
}
