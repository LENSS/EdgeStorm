package nimbusscheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.omg.CosNaming.NamingContextPackage.NotEmpty;

import topology.Topology;
import utils.Helper;
import utils.Serialization;
import cluster.Cluster;
import cluster.Node;
import zookeeper.Assignment;

public class RoundRobinScheduling {
	String submitterAddr;
	Topology t;
	Cluster cluster;
	
	public RoundRobinScheduling(String submitterNode, Topology topology, Cluster clus) {
		submitterAddr = submitterNode;
		t = topology;
		cluster = clus;
	}
		
	public Assignment schedule(){
		
		Assignment newAssign = new Assignment();
		
		// Report IP addresses of available nodes to clients, such that they can calculate the RTT time to each other
		List<String> availableNodes = cluster.getAvailableCompNodes();					
		for (String addr: availableNodes){
			newAssign.addAddress(addr);
		}
		
		// set assignment metric to be maximum double
		newAssign.setAssignMetric(Double.MAX_VALUE);   
		
		// Set the submitterIP
		newAssign.setSourceAddr(submitterAddr);
		
		// Set service topology
		String serTopology = Serialization.Serialize(t);
		newAssign.setSerTopology(serTopology);
		
		// Assign tasks to computing nodes
		String[] topComponents = t.getComponents();
		String spoutComponent = topComponents[0];		
		int componentNum = t.getComponentNum();
		int nodeNum = availableNodes.size();
		int freeNodeIndex = 0;
		HashMap<String,Integer> paraHints=t.getParallelismHints();
		HashMap<String,Integer> scheduleRequirements = t.getScheduleRequirements();
		for(int i=0;i<componentNum;i++){
			String component=topComponents[i];
			int parallelNum = paraHints.get(component);
			for(int j=0;j<parallelNum;j++)
			{	
				
				int taskid = cluster.getNextTaskId();
				String node;
				if(scheduleRequirements.get(component) == Topology.Schedule_Local) {
					node = submitterAddr;
				}
				else {
					node = availableNodes.get(freeNodeIndex%nodeNum);
				}
				if(component.equals(spoutComponent)){
					newAssign.addSpoutAddr(node);		// set the spoutAddresses
				}
				newAssign.assgin(node, taskid, component);
				// update the node set that receives tasks [update 2018/03/21]
				newAssign.addAssginedNodes(node);
				freeNodeIndex++;
			}
		}
		
		// Assign port to task pair
//		HashMap<Integer,String> task2Comp = newAssign.getTask2Component();
//		Iterator<Entry<Integer,String>> it = task2Comp.entrySet().iterator();
//		while(it.hasNext())
//		{			
//			Entry<Integer,String> entry=(Entry<Integer,String>)it.next();
//			int taskID = entry.getKey();
//			String component = entry.getValue();
//			ArrayList<String> downStreamComponents = t.getDownStreamComponents(component);
//			for (int i=0; i<downStreamComponents.size();i++){
//				String downComponent = downStreamComponents.get(i);
//				ArrayList<Integer> downStreamtaskIDs = newAssign.getComponent2Tasks().get(downComponent);
//				for (int j=0; j<downStreamtaskIDs.size();j++){
//					newAssign.assignPort(Helper.getNextPort(), taskID, downStreamtaskIDs.get(j));
//				}
//			}
//		}
		
		// Tell assigned nodes how to connect each other
		List<String> assignNodes = newAssign.getAssginedNodes();
		HashMap<String, Integer> assignNode2AddrStatus = new HashMap<String, Integer>();
		HashMap<String, Integer> assignNode2Index = new HashMap<String, Integer>();
		int index = 0;
		for(String assignNode:assignNodes) {
			int nodeID = cluster.getNodeIDByAddr(assignNode);
			Node node = cluster.getNodeByNodeId(nodeID);
			assignNode2AddrStatus.put(assignNode,node.getAddrStatus());
			assignNode2Index.put(assignNode, index);
			index++;
		}
		int assignNodeSize = assignNodes.size();
		int[][] node2NodeConnection = new int[assignNodeSize][assignNodeSize];
		HashMap<Integer,String> task2Component = newAssign.getTask2Component();
		for(Integer taskID: task2Component.keySet()) {
			String component = task2Component.get(taskID);
			String taskNode = newAssign.getTask2Node().get(taskID);
			int taskNodeAddrStatus = assignNode2AddrStatus.get(taskNode);
			int taskNodeIndex = assignNode2Index.get(taskNode);
			ArrayList<String> downStreamComponents = t.getDownStreamComponents(component);
			for (int i=0; i<downStreamComponents.size();i++){
				String downComponent = downStreamComponents.get(i);
				ArrayList<Integer> downStreamtaskIDs = newAssign.getComponent2Tasks().get(downComponent);
				for (int j=0; j<downStreamtaskIDs.size();j++){
					int downStreamTaskID = downStreamtaskIDs.get(j);
					String downStreamTaskNode = newAssign.getTask2Node().get(downStreamTaskID);
					int downStreamTaskNodeAddrStatus = assignNode2AddrStatus.get(downStreamTaskNode);
					int downStreamTaskNodeIndex = assignNode2Index.get(downStreamTaskNode);
					// Algorithm provided by Amran Haroon
					if(taskNodeAddrStatus == Node.PRIVATE) {
						if(node2NodeConnection[downStreamTaskNodeIndex][taskNodeIndex] != 1)
							node2NodeConnection[taskNodeIndex][downStreamTaskNodeIndex] = 1;
					} else {
						if(node2NodeConnection[downStreamTaskNodeIndex][taskNodeIndex] != 1) {
							if(downStreamTaskNodeAddrStatus == Node.PRIVATE)
								node2NodeConnection[downStreamTaskNodeIndex][taskNodeIndex] = 1;
							else
								node2NodeConnection[taskNodeIndex][downStreamTaskNodeIndex] = 1; 
						}
					}
				}
			}
		}	
		newAssign.setNode2NodeConnection(node2NodeConnection);	
		
		return newAssign;
	}
}
