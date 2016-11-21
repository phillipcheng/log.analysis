package etl.flow.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.JsonUtil;
import bdap.util.SftpUtil;
import etl.flow.ActionNode;
import etl.flow.Data;
import etl.flow.EndNode;
import etl.flow.ExeType;
import etl.flow.Flow;
import etl.flow.InputFormatType;
import etl.flow.Link;
import etl.flow.LinkType;
import etl.flow.Node;
import etl.flow.NodeLet;
import etl.flow.PersistType;
import etl.flow.StartNode;
import etl.flow.deploy.FlowDeployer;
import etl.flow.test.oozie.TestOozieFlow;

public class FlowTest {
	public static final Logger logger = LogManager.getLogger(FlowTest.class);
	
	private FlowDeployer deployer = new FlowDeployer();
	
	@Test
	public void installFirstTimeEngine() throws Exception{
		deployer.installEngine(true);
	}
	
	
	@Test
	public void updateEngine() throws Exception{
		deployer.installEngine(false);
	}

	public static Flow getFlow1(){
		String wfName="flow1";
		Flow flow = new Flow(wfName);
		flow.putProperty(Flow.key_wfName, wfName);
		//action nodes
		List<Node> actionNodes = new ArrayList<Node>(); 
		StartNode start = new StartNode(5*60);
		EndNode end = new EndNode();
		actionNodes.add(start);
		actionNodes.add(end);
		{//sftp action
			ActionNode sftp = new ActionNode("sftp", ExeType.mr, InputFormatType.Line, wfName+"/action_sftp.properties");
			sftp.putProperty(ActionNode.key_cmd_class, "etl.cmd.SftpCmd");
			sftp.addInLet(new NodeLet("0", "sftp.map"));
			actionNodes.add(sftp);
		}{//csv transform action
			ActionNode csvTrans = new ActionNode("csvtransform", ExeType.mr, InputFormatType.File, wfName+"/action_csvtransform.properties");
			csvTrans.putProperty(ActionNode.key_cmd_class, "etl.cmd.CsvTransformCmd");
			csvTrans.addInLet(new NodeLet("0", "data1"));
			csvTrans.addOutLet(new NodeLet("0", "data1trans"));
			actionNodes.add(csvTrans);
		}{//csv transform action
			ActionNode csvMerge = new ActionNode("csvmerge", ExeType.mr, InputFormatType.File, wfName+"/action_csvmerge.properties");
			csvMerge.putProperty(ActionNode.key_cmd_class, "etl.cmd.CsvMergeCmd");
			csvMerge.addInLet(new NodeLet("0", "data1trans"));
			csvMerge.addInLet(new NodeLet("1", "data2"));
			csvMerge.addOutLet(new NodeLet("0", "csvmerge"));
			actionNodes.add(csvMerge);
		}
		flow.setNodes(actionNodes);
		//data
		List<Data> data= new ArrayList<Data>();
		data.add(new Data("sftp.map", "/flow1/sftpcfg/test1.sftp.map.properties", InputFormatType.Line, PersistType.FileOrMem, false));
		data.add(new Data("data1", "/flow1/data1/", InputFormatType.File, PersistType.FileOrMem));
		data.add(new Data("data2", "/flow1/data2/", InputFormatType.File, PersistType.FileOrMem));
		data.add(new Data("data1trans", "/flow1/csvtrans/", InputFormatType.Line, PersistType.FileOrMem));
		data.add(new Data("csvmerge", "/flow1/csvmerge/", InputFormatType.Line, PersistType.FileOrMem));
		flow.setData(data);
		
		//links
		List<Link> links = new ArrayList<Link>();
		links.add(new Link(StartNode.start_node_name, "sftp"));
		links.add(new Link("sftp", "csvtransform", LinkType.success));
		links.add(new Link("csvtransform", "csvmerge", LinkType.success));
		links.add(new Link("csvmerge", EndNode.end_node_name));
		flow.setLinks(links);
		
		return flow;
	}
	/**
	 *        - action 1 -
	 *  start              end
	 *        - action 2 -
	 **/
	public static Flow getFlow2(){
		Flow flow = new Flow();
		List<Node> nl = new ArrayList<Node>();
		List<Link> ll = new ArrayList<Link>();
		nl.add(new StartNode());
		nl.add(new EndNode());
		flow.setNodes(nl);
		flow.setLinks(ll);
		ActionNode n1 = new ActionNode("act1");
		ActionNode n2 = new ActionNode("act2");
		nl.add(n1);
		nl.add(n2);
		Link l1 = new Link(StartNode.start_node_name, "act1");
		Link l2 = new Link(StartNode.start_node_name, "act2");
		Link l3 = new Link("act1", EndNode.end_node_name);
		Link l4 = new Link("act2", EndNode.end_node_name);
		ll.add(l1);
		ll.add(l2);
		ll.add(l3);
		ll.add(l4);
		return flow;
	}
	
	/**
	 *        - action 1 - action 3------
	 *  start                            end
	 *        - action 2 -
	 **/
	public static Flow getFlow3(){
		Flow flow = new Flow();
		List<Node> nl = new ArrayList<Node>();
		List<Link> ll = new ArrayList<Link>();
		nl.add(new StartNode());
		nl.add(new EndNode());
		flow.setNodes(nl);
		flow.setLinks(ll);
		ActionNode n1 = new ActionNode("act1");
		ActionNode n2 = new ActionNode("act2");
		ActionNode n3 = new ActionNode("act3");
		nl.add(n1);
		nl.add(n2);
		nl.add(n3);
		Link l1 = new Link(StartNode.start_node_name, "act1");
		Link l2 = new Link(StartNode.start_node_name, "act2");
		Link l3 = new Link("act3", EndNode.end_node_name);
		Link l4 = new Link("act2", EndNode.end_node_name);
		Link l5 = new Link("act1", "act3");
		ll.add(l1);
		ll.add(l2);
		ll.add(l3);
		ll.add(l4);
		ll.add(l5);
		return flow;
	}
	
	/**
	 *                     action 3
	 *        - action 1 - ------
	 *                     action 4
	 *  start                            end
	 *        - action 2 ----------
	 **/
	public static Flow getFlow4(){
		Flow flow = new Flow();
		List<Node> nl = new ArrayList<Node>();
		List<Link> ll = new ArrayList<Link>();
		nl.add(new StartNode());
		nl.add(new EndNode());
		flow.setNodes(nl);
		flow.setLinks(ll);
		ActionNode n1 = new ActionNode("act1");
		ActionNode n2 = new ActionNode("act2");
		ActionNode n3 = new ActionNode("act3");
		ActionNode n4 = new ActionNode("act4");
		nl.add(n1);
		nl.add(n2);
		nl.add(n3);
		nl.add(n4);
		Link l1 = new Link(StartNode.start_node_name, "act1");
		Link l2 = new Link(StartNode.start_node_name, "act2");
		Link l3 = new Link("act4", EndNode.end_node_name);
		Link l4 = new Link("act3", EndNode.end_node_name);
		Link l5 = new Link("act2", EndNode.end_node_name);
		Link l6 = new Link("act1", "act3");
		Link l7 = new Link("act1", "act4");
		ll.add(l1);
		ll.add(l2);
		ll.add(l3);
		ll.add(l4);
		ll.add(l5);
		ll.add(l6);
		ll.add(l7);
		return flow;
	}
	
	@Test
	public void testToDAG1(){
		Flow flow = getFlow1();
		flow.init();
		List<Node> nl = flow.getTopoOrder();
		logger.info(nl);
	}
	
	@Test
	public void testToDAG2(){
		Flow flow = getFlow2();
		flow.init();
		List<Node> nl = flow.getTopoOrder();
		logger.info(nl);
		String expected = "[[act2], [act1]]";
	}
	
	@Test
	public void testToDAG3(){
		Flow flow = getFlow3();
		flow.init();
		List<Node> nl = flow.getTopoOrder();
		logger.info(nl);
		String expected="[[act2], [act1, act3]]";
	}
	
	@Test
	public void testToDAG4(){
		Flow flow = getFlow4();
		flow.init();
		List<Node> nl = flow.getTopoOrder();
		logger.info(nl);
	}
	
	@Test
	public void genFlow1Json(){
		JsonUtil.toLocalJsonFile(getRelativeResourceFolder() + "flow1.json", FlowTest.getFlow1());
	}
	
	public void initData(){
		//setup data
		String sftpUser="dbadmin";
		String sftpPasswd="password";
		SftpUtil.sftpFromLocal(deployer.getOC().getOozieServerIp(), 22, sftpUser, sftpPasswd, String.format("%sdata", getRelativeResourceFolder()), 
				String.format("/data/flow1/"));
		try {
			deployer.getFs().copyFromLocalFile(new Path(String.format("%sdata/sftpcfg/test1.sftp.map.properties", getRelativeResourceFolder())), 
					new Path("/flow1/sftpcfg/test1.sftp.map.properties"));
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public String getRelativeResourceFolder() {
		return "src/test/resources/flow1/";
	}
}
