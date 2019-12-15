package nimbusscheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import Jama.Matrix;
import cluster.Cluster;
import cluster.Node;
import topology.Topology;
import utils.Helper;
import utils.Serialization;
import zookeeper.Assignment;

public class ResilientScheduling {
	String submitterAddr;
	Topology t;
	Cluster cluster;
	
	Logger logger = Logger.getLogger("ResilientScheduling");
	
	public ResilientScheduling(String submitterNode, Topology topology, Cluster clus) {
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
		
		// Set the submitterIP
		newAssign.setSourceAddr(submitterAddr);
		
		// Set service topology
		String serTopology = Serialization.Serialize(t);
		newAssign.setSerTopology(serTopology);
			
		//// get t2tTraffic, compParallelism and task2CompName
		HashMap<String,Integer> parallelism = t.getParallelismHints();
		int compNum = t.getComponentNum();
		String[] components = t.getComponents();
		int[] compParallelism = new int[compNum-2];
		List<String> taskCompName = new ArrayList<String>();
		int totalTaskNum = 0;
		for(int i = 0; i < compNum-2; i++) {
			String compName = components[i+1];
			compParallelism[i] = parallelism.get(compName);
			for(int j = 0; j < compParallelism[i]; j++) {
				taskCompName.add(compName);
			}
			totalTaskNum += compParallelism[i];
		}
		double[][] t2tTraffic = new double[totalTaskNum][totalTaskNum];
		for(int i = 0; i < totalTaskNum; i++) {
			for(int j = 0; j < totalTaskNum; j++) {
				t2tTraffic[i][j] = 10.0;
			}
		}
		
		// get exptExecutorOfDevice and devAvailability
		int devNum = availableNodes.size();
		int [] exptExecutorsOfDevice = new int[devNum];
		double[] devAvailability = new double[devNum];
		for(int i = 0; i < availableNodes.size(); i++) {
			String nodeAddr = availableNodes.get(i);
			int nodeId = cluster.getNodeIDByAddr(nodeAddr);
			Node node = cluster.getNodeByNodeId(nodeId);
			devAvailability[i]=node.getNodeAvailibility();
			if(nodeAddr.equals(submitterAddr))
				exptExecutorsOfDevice[i] = 0;
			else
				exptExecutorsOfDevice[i] = 4;
		}
		
		
		REMStormAdvancedGeneticSearch adrs = new REMStormAdvancedGeneticSearch(t2tTraffic, compParallelism, exptExecutorsOfDevice, devAvailability);
		Matrix bestSchedule = adrs.search();
		
		if(bestSchedule!=null) {
			//// Assign tasks to computing nodes according to the scheduling results
			
			// add at least one task on submitter
			for(int i = 0; i < compNum; i++) {
				String component = components[i];
				newAssign.assgin(submitterAddr, cluster.getNextTaskId(), component);
				newAssign.addAssginedNodes(submitterAddr);
			}
			
			double[][] bestScheduleArray = bestSchedule.getArray();
			for(int i = 0; i < totalTaskNum; i++){
				for(int j = 0; j < devNum;j++){
					if(bestScheduleArray[i][j]==1){
						String node = availableNodes.get(j);
						String compName = taskCompName.get(i);
						int taskID = cluster.getNextTaskId();
						newAssign.assgin(node, taskID, compName);
						newAssign.addAssginedNodes(node);
					}
				}
			}
			logger.info(newAssign.getTask2Node());
			
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
						// Algorithm provided by Amran
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
		}		
		return newAssign;
	}
}
