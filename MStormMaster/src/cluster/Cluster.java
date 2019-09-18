package cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import masternode.MasterNode;
import topology.Topology;
import utils.Serialization;
import zookeeper.Assignment;



public class Cluster {
	//// FOR ALL CLUSTERS
	public static HashMap<Integer,Cluster> clusters = new HashMap<Integer, Cluster>();
	private static int MAX_CLUSTERS_ID =100;
	private static int MAX_NODES_PER_CLUSTER = 500;
	private static int lastClusterId = 0;			// Latest cluster ID assigned

	//// FOR EACH CLUSTER
	private int clusterId;
	private int lastTopologyId; // Last topology ID assigned
	private int lastTaskId;		// Last task ID assigned
	private int lastNodeId;		// Last node ID assigned
	private HashMap<String, Boolean> nodeAddr2Avail; 				// Record if a node is available
	private HashMap<String, Integer> addr2nodeID;					// Check ID of a node with address
	private HashMap<Integer, Node> nodeID2node; 					// Get node with ID
	private HashMap<Integer, Integer> nodeID2TopologyID; 			// Check the topology a node belongs to
	private HashMap<Integer,Topology> topologyID2topology; 			// Get a topology from the topology ID
	private HashMap<Integer,Assignment> topologyID2assignment; 		// Get the assignment of a topology
	private HashMap<Integer,Boolean> topologyID2BeingRescheduled; 	// Check if a topology is being rescheduled


	//// METHODS FOR ALL CLUSTERS
	public static Cluster getCluster(){
		if(lastClusterId != 0 && clusters.get(lastClusterId).getNodeNum()<MAX_NODES_PER_CLUSTER) { // current cluster can still add more nodes
			return clusters.get(lastClusterId);
		}
		else { // create new cluster
			if(lastClusterId < MAX_CLUSTERS_ID){
				++lastClusterId;
				Cluster newCluster=new Cluster(lastClusterId);
				clusters.put(lastClusterId, newCluster);
				return newCluster;
			}
			else {
				return null;
			}
		}
	}

	public static Cluster getClusterById(int id){
		return clusters.get(id);
	}

	public static void removeClusterById (int id){
		clusters.remove(id);
	}

	public static Cluster getClusterByNodeAddress(String addr){
		for(Map.Entry<Integer, Cluster> clusterEntry:clusters.entrySet()){
			Cluster cluster = clusterEntry.getValue();
			Set<String> nodeAddrs = cluster.addr2nodeID.keySet();
			if(nodeAddrs.contains(addr)){
				return cluster;
			}
		}
		return null;
	}


	//// METHODS FOR EACH CLUSTER
	public Cluster(Integer id){
		clusterId = id;
		lastTopologyId = 0;
		lastTaskId = 0;
		lastNodeId = 0;
		nodeAddr2Avail = new HashMap<String, Boolean>();
		addr2nodeID = new HashMap<String, Integer>();
		nodeID2node = new HashMap<Integer, Node>();
		nodeID2TopologyID = new HashMap<Integer, Integer>();
		topologyID2topology = new HashMap<Integer, Topology>();
		topologyID2assignment=new HashMap<Integer,Assignment>();
		topologyID2BeingRescheduled = new HashMap<Integer, Boolean>();
	}

	public int getClusterId(){
		return clusterId;
	}

	public int getNextTopologyId(){
		return ++lastTopologyId;
	}

	public int getNextTaskId(){
		return ++lastTaskId;
	}

	public int getNextNodeId(){
		if(lastNodeId<MAX_NODES_PER_CLUSTER) {
			return ++lastNodeId;
		}
		else {
			return 0;
		}
	}

	public int getNodeNum(){
		return lastNodeId;
	}

	public ArrayList<String> getAvailableCompNodes(){
		ArrayList<String> availableNodes = new ArrayList<String>();
		for (Map.Entry<String, Boolean> entry: nodeAddr2Avail.entrySet()){
			if (entry.getValue() == true){
				availableNodes.add(entry.getKey());
			}
		}
		return availableNodes;
	}

	public void updateComputingNodes(List<String> children) {
		// remove nodes that are not in the cluster any more
		HashSet<String> previousNodeAddr = new HashSet<String>();
		Iterator<Map.Entry<Integer, Node>> it = nodeID2node.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer, Node> entry = it.next();
			String addr = entry.getValue().getAddress();
			int status = entry.getValue().getAddrStatus();
			String addrAndStatus = addr + ":" + status;
			int nodeId = entry.getKey();
			if(!children.contains(addrAndStatus)){
				it.remove();
				nodeAddr2Avail.remove(addr);
				addr2nodeID.remove(addr);
				nodeID2TopologyID.remove(nodeId);
			}
			else{
				previousNodeAddr.add(addrAndStatus);
			}
		}		
		// add new nodes that are not in the cluster
		for(int i=0;i<children.size();i++){
			String nodeAddrAndStatus =children.get(i); 
			String[] nodeAddrAndStatusArray = nodeAddrAndStatus.split("\\:");
			String nodeAddr = nodeAddrAndStatusArray[0];
			int nodeStatus = new Integer(nodeAddrAndStatusArray[1]);
			if(!previousNodeAddr.contains(nodeAddrAndStatus)){
				int nodeId = getNextNodeId();
				if(nodeId!=0){
					Node newNode = new Node(nodeAddr, nodeStatus);
					nodeAddr2Avail.put(newNode.getAddress(), true);
					addr2nodeID.put(newNode.getAddress(), nodeId);
					nodeID2node.put(nodeId, newNode);
				}
				else{
					System.out.println("More than " + MAX_NODES_PER_CLUSTER + "nodes in cluster " + clusterId);
				}
			}
		}
	}

	public void setCompNodeAsUsed(String nodeAddr){
		nodeAddr2Avail.put(nodeAddr, false);
	}

	public void setCompNodeAsUnUsed(String nodeAddr){
		nodeAddr2Avail.put(nodeAddr, true);
	}

	public int getNodeIDByAddr(String addr){
		return addr2nodeID.get(addr);
	}

	public Node getNodeByNodeId(int nodeId){
		return nodeID2node.get(nodeId);
	}

	public int getTopologyIdByNodeId(int nodeId){
		return ((nodeID2TopologyID.get(nodeId)==null)? -1: nodeID2TopologyID.get(nodeId));
	}

	public Topology getTopologyByTopologyId(int topologyId){
		return topologyID2topology.get(topologyId);
	}

	public void setTopologyBeingScheduled(int topologyId, boolean state){
		topologyID2BeingRescheduled.put(topologyId, state);
	}

	public void addAssignment(int topologyID, Assignment assignment){
		Topology topology = (Topology) Serialization.Deserialize(assignment.getSerTopology(), Topology.class);
		topologyID2topology.put(topologyID, topology);
		topologyID2assignment.put(topologyID, assignment);

		// add nodeID2TopologyID pairs and computing nodes
		HashSet<String> computingNodes = new HashSet<String>(assignment.getTask2Node().values());
		for(String nodeAddr: computingNodes){
			int nodeID = addr2nodeID.get(nodeAddr);
			nodeID2TopologyID.put(nodeID, topologyID);
			setCompNodeAsUsed(nodeAddr);
		}
	}

	public void updateAssignment(int topologyID, Assignment oldAssignment, Assignment newAssignment){
		// remove old computing nodes
		HashSet<String> oldcomputingNodes = new HashSet<String>(oldAssignment.getTask2Node().values());
		for(String nodeAddr: oldcomputingNodes){
			setCompNodeAsUnUsed(nodeAddr);
		}

		// remove old nodeID2TopologyID pairs
		Iterator<Map.Entry<Integer, Integer>> it = nodeID2TopologyID.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer, Integer> entry = it.next();
			if(entry.getValue() == topologyID){
				it.remove();
			}
		}

		Topology newTopology = (Topology) Serialization.Deserialize(newAssignment.getSerTopology(), Topology.class);
		topologyID2topology.put(topologyID, newTopology);
		topologyID2assignment.put(topologyID, newAssignment);

		// add new nodeID2TopologyID pairs and computing nodes
		HashSet<String> computingNodes = new HashSet<String>(newAssignment.getTask2Node().values());
		for(String nodeAddr: computingNodes){
			int nodeID = addr2nodeID.get(nodeAddr);
			nodeID2TopologyID.put(nodeID, topologyID);
			setCompNodeAsUsed(nodeAddr);
		}

		// update the state of the status report 
		HashSet<Node> allNodes = new HashSet<Node>(nodeID2node.values());
		for(Node node: allNodes){
			node.hasBeenRescheduledWithUpdatedReport();
			node.removePhoneStatus();
		}		
	}


	public void deleteAssignment(int topologyID) {
		Assignment oldAssignment = getAssignmentByTopologyId(topologyID);

		// remove old computing nodes
		HashSet<String> oldcomputingNodes = new HashSet<String>(oldAssignment.getTask2Node().values());
		for(String nodeAddr: oldcomputingNodes){
			setCompNodeAsUnUsed(nodeAddr);
		}

		// remove old nodeID2TopologyID pairs
		Iterator<Map.Entry<Integer, Integer>> it = nodeID2TopologyID.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer, Integer> entry = it.next();
			if(entry.getValue() == topologyID){
				it.remove();
			}
		}
		
		topologyID2topology.remove(topologyID);
		topologyID2assignment.remove(topologyID);
		
		// update the state of the status report 
		HashSet<Node> allNodes = new HashSet<Node>(nodeID2node.values());
		for(Node node: allNodes){
			node.hasBeenRescheduledWithUpdatedReport();
			node.removePhoneStatus();
		}
		
		MasterNode.getInstance().mZkClient.getDM().deleteAssignment(oldAssignment.getAssignId(), getClusterId());
	}


	public Boolean meetConditionForReScheduling(int topologyID){
		// check if receive the new status report from all nodes
		HashSet<Node> nodes = new HashSet<Node>(nodeID2node.values());
		for(Node node: nodes){
			if (!node.isStatusReportUpdated()){
				return false;
			}
		}

		Assignment curAssignment = topologyID2assignment.get(topologyID);
		// check all nodes belonging to the topology to see if they report the task execution report
		HashSet<String> nodesForTopology = new HashSet<String>(curAssignment.getTask2Node().values());
		for(String str: nodesForTopology){
			Node node = nodeID2node.get(addr2nodeID.get(str));
			if (!node.isReportContainingTaskInfor()){
				return false;
			}
		}

		// ensure that the topology is not being rescheduled currently
		if(topologyID2BeingRescheduled.get(topologyID)){
			return false;
		}

		return true;	
	}

	public Assignment getAssignmentByTopologyId(int topologyID){
		return topologyID2assignment.get(topologyID);
	}

}
