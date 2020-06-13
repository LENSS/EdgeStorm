package nimbusscheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import Jama.Matrix;
import amvp.Model;
import amvp.ModelCatlog;
import amvp.SplittedModel;
import cluster.Cluster;
import cluster.Node;
import topology.Topology;
import utils.Serialization;
import zookeeper.Assignment;

public class AMVPScheduling {
	
	String submitterAddr;
	Topology t;
	Cluster cluster;
	
	Logger logger = Logger.getLogger("AMVPScheduling");
	
	public AMVPScheduling(String submitterNode, Topology topology, Cluster clus) {
		submitterAddr = submitterNode;
		t = topology;
		cluster = clus;
	}
	
	public Assignment schedule() {
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
		
		
		//// get task2CompName
		HashMap<String,Integer> parallelism = t.getParallelismHints();
		int compNum = t.getComponentNum();
		String[] components = t.getComponents();
		int[] compParallelism = new int[compNum-2];
		List<String> taskCompName = new ArrayList<String>();
		int taskNum = 0;
		for(int i = 0; i < compNum-2; i++) {
			String compName = components[i+1];
			compParallelism[i] = parallelism.get(compName);
			for(int j = 0; j < compParallelism[i]; j++) {
				taskCompName.add(compName);
			}
			taskNum += compParallelism[i];
		}
		int devNum = availableNodes.size();
		
		//// For AMVP Paper only
		// The order is in gender, emotion, age !!!!!!!!		
		// Different conditions
		double[] accReq = {90,90,90};
		double[] lanReq = {1000, 1000, 1000};
		double[] thrReq = {1,1,1};
		double[] alpha = {0.6,0.6,0.6};
		double[] belta = {0.2,0.2,0.2};
		double[] gamma = {0.2,0.2,0.2};
		double[] cpuRes = {4,4};
		//Arrays.fill(cpuRes, 1);
		double[] memRes = new double[devNum];
		Arrays.fill(memRes, 500);
		double[] netBW = new double[devNum];
		Arrays.fill(netBW, 1);
		//// For AMVP Paper only
		
		
		// results
		Matrix bestSchedule = null;
		double bestMetric = Double.MAX_VALUE;
		List<SplittedModel> selectedModels = null;	
		
		ModelCatlog modelCatlog = new ModelCatlog();
		
		// For mobileNet case
		Model genderModel = modelCatlog.secondTo(null, "gender", "mobileNetV2");
		Model emotionModel = modelCatlog.secondTo(null, "emotion", "mobileNetV2");
		Model ageModel = modelCatlog.secondTo(null, "age", "mobileNetV2");
		
		while (genderModel!= null && emotionModel!=null && ageModel!=null) {
			System.out.println("I AM HERE!");
			List<Model> models = new ArrayList<>();
			models.add(genderModel);
			models.add(emotionModel);
			models.add(ageModel);
			List<SplittedModel> splittedModels = modelCatlog.getSplittedModels(models);
			
			AMVPGeneticSearch amcpgs = new AMVPGeneticSearch(accReq, lanReq, thrReq, alpha, belta, gamma, cpuRes, memRes, netBW, splittedModels, taskNum, devNum);
			Matrix localBestSchedule = amcpgs.search();
			double localBestMetric = amcpgs.getBestMetric();
			
			if(localBestMetric < bestMetric) {
				bestMetric = localBestMetric;
				bestSchedule = localBestSchedule;
				selectedModels = splittedModels;
				amcpgs.printSeparateMertic();
			}
			
			if(amcpgs.getEarlyExit())
				break;
			
			double gender_acc_metric = alpha[0]*(lanReq[0]-genderModel.accuracy);
			double emotion_acc_metric = alpha[1]*(lanReq[1]-emotionModel.accuracy);
			double age_acc_metric = alpha[2]*(lanReq[2]-ageModel.accuracy);
			double min_acc_cost = Math.min(Math.min(gender_acc_metric, emotion_acc_metric),age_acc_metric);
			if(min_acc_cost == gender_acc_metric) {
				genderModel = modelCatlog.secondTo(genderModel, "gender", "mobileNetV2");
			} else if(min_acc_cost == emotion_acc_metric) {
				emotionModel = modelCatlog.secondTo(emotionModel, "emotion", "mobileNetV2");
			} else {
				ageModel = modelCatlog.secondTo(ageModel, "age", "mobileNetV2");
			}
		}
		
		// For resNet case
		Model genderModelRes = modelCatlog.secondTo(null, "gender", "resNet50V2");
		Model emotionModelRes = modelCatlog.secondTo(null, "emotion", "resNet50V2");
		Model ageModelRes = modelCatlog.secondTo(null, "age", "resNet50V2");
		
		while (genderModelRes!=null && emotionModelRes !=null && ageModelRes!=null) {
			System.out.println("I AM HERE2!");
			List<Model> models = new ArrayList<>();
			models.add(genderModelRes);
			models.add(emotionModelRes);
			models.add(ageModelRes);
			List<SplittedModel> splittedModels = modelCatlog.getSplittedModels(models);
			
			AMVPGeneticSearch amcpgs = new AMVPGeneticSearch(accReq, lanReq, thrReq, alpha, belta, gamma, cpuRes, memRes, netBW, splittedModels, taskNum, devNum);
			Matrix localBestSchedule = amcpgs.search();
			double localBestMetric = amcpgs.getBestMetric();
			
			if(localBestMetric < bestMetric) {
				bestMetric = localBestMetric;
				bestSchedule = localBestSchedule;
				selectedModels = splittedModels;
				amcpgs.printSeparateMertic();
			}
			
			if(amcpgs.getEarlyExit())
				break;
			
			double gender_acc_metric = alpha[0]*(lanReq[0]-genderModelRes.accuracy);
			double emotion_acc_metric = alpha[1]*(lanReq[1]-emotionModelRes.accuracy);
			double age_acc_metric = alpha[2]*(lanReq[2]-ageModelRes.accuracy);
			double min_acc_cost = Math.min(Math.min(gender_acc_metric, emotion_acc_metric),age_acc_metric);
			if(min_acc_cost == gender_acc_metric) {
				genderModelRes = modelCatlog.secondTo(genderModelRes, "gender", "resNet50V2");
			} else if(min_acc_cost == emotion_acc_metric) {
				emotionModelRes = modelCatlog.secondTo(emotionModelRes, "emotion", "resNet50V2");
			} else {
				ageModelRes = modelCatlog.secondTo(ageModelRes, "age", "resNet50V2");
			}
		}
		
		if(bestSchedule!=null) {			
	        // Assign task of the first and last component to the submitter
			String spoutComponent = components[0];		
			newAssign.assgin(submitterAddr, cluster.getNextTaskId(), spoutComponent);
			String LastComponent = components[compNum-1];
			newAssign.assgin(submitterAddr, cluster.getNextTaskId(), LastComponent);
			newAssign.addSpoutAddr(submitterAddr);
			newAssign.addAssginedNodes(submitterAddr);
						
			//// Assign tasks to computing nodes according to the scheduling results
			double[][] bestScheduleArray = bestSchedule.getArray();
			for(int i = 0; i < taskNum; i++){
				for(int j = 0; j < devNum;j++){
					if(bestScheduleArray[i][j]==1){
						String node = availableNodes.get(j);
						String compName = taskCompName.get(i);
						int taskID = cluster.getNextTaskId();
						newAssign.assgin(node, taskID, compName);
						newAssign.addAssginedNodes(node);
						SplittedModel model = selectedModels.get(i);
						String variant = "";
						if(!model.type.equals( "common"))
							variant = model.baseType + "_frozen" + model.frozenPoint + "_split" + model.splitPoint + "_" + model.part;
						else
							variant = model.baseType + "_split" + model.splitPoint + "_" + model.part;
						System.out.println("taskID:" + taskID + ", modelVariant:" + variant);
						newAssign.setTask2ModelVariant(taskID, variant);
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
			return newAssign;
		} else {	// cannot be scheduled
			return null;
		}
	}
}
