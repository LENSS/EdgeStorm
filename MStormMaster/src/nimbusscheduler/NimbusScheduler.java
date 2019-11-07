package nimbusscheduler;


import java.util.ArrayList;

import topology.Topology;
import utils.Serialization;
import zookeeper.Assignment;
import cluster.Cluster;


import masternode.MasterNode;
import communication.Request;

public class NimbusScheduler {

	public NimbusScheduler() {
	}

	public Assignment fstSchedule(Request req) {
		// Use the cluster that the task submitter belongs to
		String submitterAddress = req.getGUID();		
		Cluster cluster = Cluster.getClusterByNodeAddress(submitterAddress);
		
		String serTopology = req.getContent();
		Topology topology= (Topology) Serialization.Deserialize(serTopology, Topology.class);
		
		RoundRobinScheduling rrs = new RoundRobinScheduling(submitterAddress,topology,cluster);
		Assignment newAssign = rrs.schedule();
		
//		ResilientScheduling resSch = new ResilientScheduling(submitterAddress, topology, cluster);
//		Assignment newAssign = resSch.schedule();
		
		if(newAssign!=null){	// can be scheduled
			int topologyId=cluster.getNextTopologyId();	 // assign a unique id for this topology/assignment		
			newAssign.setAssignId(topologyId);			// topology id is also used as assignment id
			newAssign.setApk(req.getFileName());
			newAssign.setAssignType(Assignment.FSTSCHE);
			cluster.addAssignment(topologyId,newAssign);
			MasterNode.getInstance().mZkClient.getDM().addNewAssignment(newAssign,cluster.getClusterId());
			return newAssign;
		} else{		// cannot be scheduled
			System.out.println("The topology from GUID: " + submitterAddress + "cannot be schduled!\n");
			return null;
		}
	}
	
	public void reSchedule(int topologyId, Cluster cluster) {
		AdvancedScheduling as = new AdvancedScheduling(topologyId, cluster, AdvancedScheduling.RESOURCE_S);
		double[] oldAssignMetricsInCurrentEnvironment = as.getMetricsOfPreAllocation();
		System.out.println("Time: " + "\t" + System.nanoTime() + "\t" +
		                   "delayMetric: " + "\t" + oldAssignMetricsInCurrentEnvironment[0] + "\t" +
						   "energyMetric: " + "\t" + oldAssignMetricsInCurrentEnvironment[1]);
		
		if(cluster.getAssignmentByTopologyId(topologyId).getAssignType() == Assignment.FSTSCHE){	// The First Reschedule Always Happens
				Assignment newAssign = as.schedule();
				newAssign.setAssignId(topologyId);		// topology id is also used as assignment id
				Assignment oldAssign = cluster.getAssignmentByTopologyId(topologyId);
				newAssign.setApk(oldAssign.getApk());
				newAssign.setAssignType(Assignment.RESCHE);
				newAssign.setPreAssignedNodes(oldAssign.getAssginedNodes());
				cluster.updateAssignment(topologyId,oldAssign, newAssign);
				MasterNode.getInstance().mZkClient.getDM().updateAssignment(newAssign,cluster.getClusterId());
		} else {	// 2nd~nth reschedule based on feedback information			
			if (as.meetRescheduleCondition()){
				Assignment newAssign = as.schedule();
				newAssign.setAssignId(topologyId);		// topology id is also used as assignment id
				Assignment oldAssign = cluster.getAssignmentByTopologyId(topologyId);
				newAssign.setApk(oldAssign.getApk());
				newAssign.setAssignType(Assignment.RESCHE);
				newAssign.setPreAssignedNodes(oldAssign.getAssginedNodes());
				cluster.updateAssignment(topologyId,oldAssign,newAssign);
				MasterNode.getInstance().mZkClient.getDM().updateAssignment(newAssign,cluster.getClusterId());
			}
		}
	}
}
