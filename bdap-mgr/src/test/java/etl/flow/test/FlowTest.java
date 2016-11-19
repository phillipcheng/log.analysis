package etl.flow.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.flow.ActionNode;
import etl.flow.EndNode;
import etl.flow.Flow;
import etl.flow.Link;
import etl.flow.Node;
import etl.flow.StartNode;
import etl.flow.deploy.FlowDeployer;
import etl.flow.test.oozie.TestOozieFlow;

public class FlowTest {
	public static final Logger logger = LogManager.getLogger(FlowTest.class);
	
	private FlowDeployer deployer = new FlowDeployer();
	
	//@Test
	public void installFirstTimeEngine() throws Exception{
		deployer.installEngine(true);
	}
	//@Test
	public void updateEngine() throws Exception{
		deployer.installEngine(false);
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
		Flow flow = TestOozieFlow.getFlow1();
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
}
