package nimbusscheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.omg.CORBA.PUBLIC_MEMBER;

import Jama.Matrix;
import status.ReportToNimbus;
import topology.Topology;
import utils.Helper;
import utils.Serialization;
import zookeeper.Assignment;
import cluster.Cluster;
import cluster.Node;

public class AdvancedScheduling {
	public static final int FEEDBACK_S = 0;
	public static final int RESOURCE_S = 1;
	public static final int TRAFFIC_S = 2;
	public static final int RESILIENT_S = 3;
	
	private final double LOW_BOUND = 0.5;
	private final double UP_BOUND = 1;
	private final double compRatio = 1;
	private final double devResRatio = 1;
	
	private final double COMP_THRESHOLD = 10;
	private final double TASK_THRESHOLD = 10;
	private final double E2EDELAY_THRESHOLD = 1500; //ms
	private final double RESCHEDULE_THRESHOLD = 1.5;
	
	private final double BYTES_TO_BITS = 8.0;
	
	String submitterAddr;
	Topology t;
	Cluster cluster;
	int topologyId;
	int schType;
	
	String[] allAvailableDevice;
	int devNum;
	double[] devAvailResource;
	double[] devCPUCapcity;
	int[] devCPUCoreNum;
	double[] devAvailability;
	public int [] exptExecutorsOfDevice;
	public double[][] d2dPropDelay;
	public double[][] d2dTransDelay;
	public double[][] d2dEnergyPerbit;
	public double[] dBattery;
	public ArrayList<Integer> currentTasks;
	public double[][] measuredt2tOutputRate;
	public double[][] measuredt2tPktAvgSize;
	public double[][] measuredt2tTraffic;
	public double[][] preAllocation;
	
	int compNum;
	String[] compNames;
	double[] compWorkload;
	double[] compExpectedInputRate;
	double[] compActualOutputRate;	
	double[][] comp2CompSpeed;
	double[][] comp2CompPktSize;
	
	// new task number for each component
	HashMap<String, Integer> compName2TaskNum;
	int[] compParallelism;
	HashMap<String, Integer> compName2InitTaskIndex;
	List<String> taskCompName;
	int totalTaskNum;
	public double[][] t2tOutputRate;
	public double[][] t2tPktAvgSize;
	public double[][] t2tTraffic;
	
	public AdvancedScheduling(int topoId, Cluster clus, int type) {
		cluster = clus;
		topologyId = topoId;
		schType = type;
		submitterAddr = clus.getAssignmentByTopologyId(topologyId).getSourceAddr();
		t = clus.getTopologyByTopologyId(topologyId);
		updateStatusMatrix();
	}
	
	public void getDevCPUInfo(){	//// GET AVAILABLE CPU RESOURCES FOR MSTORM AND CPU CAPACITY AT EACH DEVICE		
		devNum = allAvailableDevice.length;
		devAvailResource = new double[devNum];
		devCPUCapcity = new double[devNum];
		devCPUCoreNum = new int[devNum];
		devAvailability = new double[devNum];
		for(int i = 0; i < devNum; i++){
			String nodeAddr = allAvailableDevice[i];
			Node node = cluster.getNodeByNodeId(cluster.getNodeIDByAddr(nodeAddr));
			ReportToNimbus report = node.getReportToNimbus();
			devAvailResource[i] = report.availCPUForMStormTasks;
			devCPUCoreNum[i]=report.cpuCoreNum;
			devCPUCapcity[i] = report.cpuFrequency * report.cpuCoreNum;
			devAvailability[i] = report.availability;
		}
	}
	
	public void getExptWorkloadOfComps(){ //// GET EXPECTED WORKLOAD OF EACH COMPONENT ACCORDING TO EXPECTED INPUT RATE
		compNum = t.getComponentNum();
		compNames = t.getComponents();
		Assignment oldAssign = cluster.getAssignmentByTopologyId(topologyId);
		compWorkload = new double[compNum];
		for (int i = 0; i < compNum; i++){
			String compName = compNames[i];	
			List<Integer> tasks = oldAssign.getComponent2Tasks().get(compName);
			int taskNum = tasks.size();
			double outputRate = 0.0;
			double workload = 0.0;
			for (int j = 0; j<taskNum; j++){
				int taskId = tasks.get(j);
				String nodeAddr = oldAssign.getTask2Node().get(taskId);
				Node node = cluster.getNodeByNodeId(cluster.getNodeIDByAddr(nodeAddr));
				ReportToNimbus report = node.getReportToNimbus();
				outputRate += report.task2Output.getOrDefault(taskId,0.0);
				workload += report.taskID2CPUUsage.getOrDefault(taskId,0.0);
			}
			compWorkload[i] = (outputRate == 0.0 || workload == 0.0) ? 1.0 : compExpectedInputRate[i]/outputRate * workload;
			
			System.out.println("compWorkload"+i+":"+compWorkload[i]);
			System.out.println("compExpectedInputRate"+i+":"+compExpectedInputRate[i]);
			System.out.println("outputRate"+i+":"+outputRate);
			System.out.println("workload"+i+":"+workload);
			System.out.println();
		}
	}
	
	public void calExeForDevAndTaskForComp(){ //// CALCULATE TASK NUMBER FOR EACH COMPONENT AND AVAILABLE EXECUTORS AT EACH DEVICE
		double executorResource = getExecutorResource(devAvailResource, devCPUCoreNum, compWorkload, devCPUCapcity, LOW_BOUND, UP_BOUND);
		
		//double executorResource = getExecutorResource(devCPUCoreNum, devCPUCapcity);
		
		// task number for each component
		compName2TaskNum = new HashMap<String, Integer>();
		compName2InitTaskIndex = new HashMap<String, Integer>();
		taskCompName = new ArrayList<String>();
		totalTaskNum = 0;
		
		for(int i = 0; i < compNum; i++){
			String compName = compNames[i];
			compName2InitTaskIndex.put(compName, totalTaskNum);
			int compTasks = Helper.approCeil(compWorkload[i]*compRatio, executorResource);
			System.out.println("compTasks"+i+":"+compTasks);
			compName2TaskNum.put(compName, compTasks);		
			for(int j = 0; j< compTasks; j++){
				taskCompName.add(compName);
			}
			totalTaskNum += compTasks;		
 		}
		// available executors at each device
		exptExecutorsOfDevice = new int[devNum];
		for(int i = 0; i < devNum; i++){
			exptExecutorsOfDevice[i] = Helper.approFloor(devAvailResource[i]*devResRatio, executorResource);
			System.out.println("exptExecutorsOfDevice"+i+":"+exptExecutorsOfDevice[i]);
		}
	}
	
	public void getTask2TaskInfo(){ //// GET THE PARAMETERS FROM TASK TO TASK
		t2tOutputRate = new double[totalTaskNum][totalTaskNum];
		t2tPktAvgSize = new double[totalTaskNum][totalTaskNum];
		t2tTraffic = new double[totalTaskNum][totalTaskNum];
        for(int i = 1; i< compNum; i++){
        	String compName = compNames[i];
        	int curCompIndex = Arrays.asList(compNames).indexOf(compName);
        	int curParallel = compName2TaskNum.get(compName);
        	List<String> preCompNames = t.getUpStreamComponents(compName);
        	for(int j = 0; j< preCompNames.size(); j++){
				String preCompName = preCompNames.get(j);
				int preCompIndex = Arrays.asList(compNames).indexOf(preCompName);
				int preParallel = compName2TaskNum.get(preCompName);
				int initIndexPre = compName2InitTaskIndex.get(preCompName);
				int initIndexCur = compName2InitTaskIndex.get(compName);
				double trafficRate = comp2CompSpeed[preCompIndex][curCompIndex]/compActualOutputRate[preCompIndex]*compExpectedInputRate[preCompIndex]/(preParallel*curParallel);
				double trafficPktSize = comp2CompPktSize[preCompIndex][curCompIndex];
				for (int p = 0; p < preParallel; p++){
					for (int c = 0; c< curParallel; c++){
						t2tOutputRate[initIndexPre+p][initIndexCur+c] = trafficRate;
						t2tPktAvgSize[initIndexPre+p][initIndexCur+c] = trafficPktSize;
						t2tTraffic[initIndexPre+p][initIndexCur+c] = trafficRate * trafficPktSize;
					}
				}
			}
        }
	}
	
	public void preSchedule(){
		getDevCPUInfo();
		getExptWorkloadOfComps();
		calExeForDevAndTaskForComp();	
		getTask2TaskInfo();
	}
	
	public Assignment schedule(){
		Assignment newAssign = new Assignment();
		
		/// CALCULATE THE EXPECTED TASKS FOR EACH COMPONENT ADJUST THE CORRESPOND TASK TO TASK INFORMATION
		preSchedule();
		        
        // Report the ip addresses of available computing nodes to clients, such that they can calculate the RTT time to each other
		for (String addr: allAvailableDevice){
			newAssign.addAddress(addr);
		}
		
		// Set the submitterIP
		newAssign.setSourceAddr(submitterAddr);
				
		// Set new parallelism hints for each component
		String oldTopologyString = Serialization.Serialize(t);
		Topology newTopology = (Topology) Serialization.Deserialize(oldTopologyString, Topology.class);
		for(int i = 0; i< compNum; i++){
	       	String compName = compNames[i];
	       	int taskNum = compName2TaskNum.get(compName);
	       	newTopology.setParallelismHints(compName,taskNum);
		}
		
		// Set service topology
		String serTopology = Serialization.Serialize(newTopology);
		newAssign.setSerTopology(serTopology);
		
		// Do schedule
		Matrix bestSchedule = null;
		switch (schType) {
			case FEEDBACK_S:  	// GENETIC ALGORITHM TO SEARCH THE BEST SCHEDULE
				FBMStormAdvancedGeneticSearch adgs = new FBMStormAdvancedGeneticSearch(d2dPropDelay, d2dTransDelay, d2dEnergyPerbit, t2tOutputRate, t2tPktAvgSize, exptExecutorsOfDevice, dBattery);
		        bestSchedule = adgs.search();
		        newAssign.setAssignMetric(adgs.getBestMetric());   // Set assignment metric (currently, feedback scheduling algorithm allows multiple reschedulings)
		        break;
			case TRAFFIC_S:		// TSTORM ALGORITHM TO SEARCH THE BEST SCHEDULE
				TStormSearch tss = new TStormSearch(d2dPropDelay, d2dTransDelay, d2dEnergyPerbit, t2tOutputRate, t2tPktAvgSize, exptExecutorsOfDevice, dBattery);
				bestSchedule = tss.search();
				break;
			case RESOURCE_S:	// RSTORM ALGORITHM TO SEARCH THE BEST SCHEDULE
		    	RStormSearch rss = new RStormSearch(d2dPropDelay, d2dTransDelay, d2dEnergyPerbit, t2tOutputRate, t2tPktAvgSize, exptExecutorsOfDevice, dBattery);
				bestSchedule =  rss.search();
				break;
			case RESILIENT_S:	// RESILIENT ALGORITHM TO SERACH THE BEST SCHEDULE
				REMStormAdvancedGeneticSearch adrs = new REMStormAdvancedGeneticSearch(t2tTraffic, compParallelism, exptExecutorsOfDevice, devAvailability);
				bestSchedule = adrs.search();
				break;
			default:
				break;
		}
		
		if(bestSchedule!=null) {
			// Assign tasks to computing nodes according to the scheduling results
			String[] topComponents = t.getComponents();
			String spoutComponent = topComponents[0];
			double[][] bestScheduleArray = bestSchedule.getArray();
			for(int i = 0; i < totalTaskNum; i++){
				for(int j = 0; j < devNum;j++){
					if(bestScheduleArray[i][j]==1){
						String node = allAvailableDevice[j];
						String compName = taskCompName.get(i);
						if(compName.equals(spoutComponent)){
							newAssign.addSpoutAddr(node);	// set the spoutAddresses
						}
						int taskID = cluster.getNextTaskId();
						newAssign.assgin(node, taskID, compName);
						newAssign.addAssginedNodes(node);
					}
				}
			}
			
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
	
	public void updateStatusMatrix(){
		//// GET ALL AVAILABLE DEVICES
		Assignment assign = cluster.getAssignmentByTopologyId(topologyId);
		List<String> assignedNodes = new ArrayList<String>(assign.getNode2Tasks().keySet());  // get nodes assigned to current topology
  		int assignedNodeNum = assignedNodes.size(); 		
		List<String> availableNodes = cluster.getAvailableCompNodes();
		int availableNodeNum = availableNodes.size();
		int devNum = assignedNodeNum + availableNodeNum;
		allAvailableDevice = new String[devNum];
		
		// Update the battery information
		dBattery = new double[devNum];
		for(int i = 0; i < assignedNodeNum; i++){
			String nodeAddr = assignedNodes.get(i);
			allAvailableDevice[i] = nodeAddr;
			int devId = cluster.getNodeIDByAddr(nodeAddr);
			Node dev = cluster.getNodeByNodeId(devId);
			ReportToNimbus report = dev.getReportToNimbus();
			dBattery[i] = report.batteryCapacity*report.batteryLevel/100.0;
		}
		
		for(int i = 0 ; i < availableNodeNum; i++){
			String nodeAddr = availableNodes.get(i);
			int index = assignedNodeNum+i;
			allAvailableDevice[index] = nodeAddr;
			int devId = cluster.getNodeIDByAddr(nodeAddr);
			Node dev = cluster.getNodeByNodeId(devId);
			ReportToNimbus report = dev.getReportToNimbus();
			dBattery[index] = report.batteryCapacity*report.batteryLevel/100.0;
		}
		
		//// GET DELAY AND ENERGY CONSUMPTION PER BIT FROM DEVICE TO DEVICE
		d2dPropDelay = new double[devNum][devNum];
		d2dTransDelay = new double[devNum][devNum];
		d2dEnergyPerbit = new double[devNum][devNum];	
		for(int i = 0; i<devNum; i++){
			String devAddr = allAvailableDevice[i];
			int devId = cluster.getNodeIDByAddr(devAddr);
			Node dev = cluster.getNodeByNodeId(devId);
			ReportToNimbus report = dev.getReportToNimbus();
			Map<String, Double> d2dPDelay = report.rttMap;
			for(int j =0; j<devNum; j++){
				String remoteDevAddr = allAvailableDevice[j];
				int remoteDevId = cluster.getNodeIDByAddr(remoteDevAddr);
				Node remoteDev = cluster.getNodeByNodeId(remoteDevId);
				ReportToNimbus remoteReport = remoteDev.getReportToNimbus();
				d2dPropDelay[i][j] = d2dPDelay.get(remoteDevAddr);
				if(i!=j){
					d2dTransDelay[i][j] = (1.0/report.txBandwidth + 1.0/remoteReport.rxBandwidth)/1000.0 * BYTES_TO_BITS;  // The transfer Time (ms) per byte
					d2dEnergyPerbit[i][j] = report.energyPerBitTx + remoteReport.energyPerBitRx;
				} else {
					d2dTransDelay[i][j] = 0.0;
					d2dEnergyPerbit[i][j] = 0.0;
				}
			}
		}
		
		//// GET TASK TO TASK OUTPUTRATE AND AVERAGE PACKET SIZE
		HashMap<Integer, String> task2Node = assign.getTask2Node();
		currentTasks = new ArrayList<Integer>();
		for (Map.Entry<Integer, String> entry: task2Node.entrySet()){
			currentTasks.add(entry.getKey());
		}
		int currentTaskNum = task2Node.size();
		measuredt2tOutputRate = new double[currentTaskNum][currentTaskNum];
	    measuredt2tPktAvgSize = new double[currentTaskNum][currentTaskNum];
	    measuredt2tTraffic = new double[currentTaskNum][currentTaskNum];
		preAllocation = new double [currentTaskNum][devNum];		
		
		for(int i = 0; i < assignedNodeNum; i++){
			String nodeAddr = assignedNodes.get(i);
			int devId = cluster.getNodeIDByAddr(nodeAddr);
			Node dev = cluster.getNodeByNodeId(devId);
			ReportToNimbus report = dev.getReportToNimbus();
			
			HashMap<Integer, Map<Integer, Double>> task2TaskOutput = (HashMap<Integer, Map<Integer, Double>>) report.task2TaskTupleRate;
			for (Map.Entry<Integer, Map<Integer, Double>> task2TaskOutputEntry:task2TaskOutput.entrySet()){
				int curTask = task2TaskOutputEntry.getKey();
				int curTaskIndex = currentTasks.indexOf(curTask);
				preAllocation[curTaskIndex][i] = 1.0;          // fill the allocation array
				HashMap<Integer, Double> task2Output = (HashMap<Integer, Double>) task2TaskOutputEntry.getValue();
				for(Map.Entry<Integer, Double> task2OutputEntry: task2Output.entrySet()){
					int downTask = task2OutputEntry.getKey();
					int downTaskIndex = currentTasks.indexOf(downTask);
					measuredt2tOutputRate[curTaskIndex][downTaskIndex] = task2OutputEntry.getValue();
				}	
			}
			
			HashMap<Integer, Map<Integer, Double>> task2TaskAvgPktSize = (HashMap<Integer, Map<Integer, Double>>) report.task2TaskTupleAvgSize;
			for (Map.Entry<Integer, Map<Integer, Double>> task2TaskAvgPktSizeEntry:task2TaskAvgPktSize.entrySet()){
				int curTask = task2TaskAvgPktSizeEntry.getKey();
				int curTaskIndex = currentTasks.indexOf(curTask);
				HashMap<Integer, Double> task2AvgPktSize = (HashMap<Integer, Double>) task2TaskAvgPktSizeEntry.getValue();
				for(Map.Entry<Integer, Double> task2AvgPktSizeEntry: task2AvgPktSize.entrySet()){
					int downTask = task2AvgPktSizeEntry.getKey();
					int downTaskIndex = currentTasks.indexOf(downTask);
					measuredt2tPktAvgSize[curTaskIndex][downTaskIndex] = task2AvgPktSizeEntry.getValue();
				}	
			}
			
			for(int row = 0; row < currentTaskNum; row++) {
				for(int col = 0; col< currentTaskNum; col++) {
					measuredt2tTraffic[row][col] = measuredt2tOutputRate[row][col]*measuredt2tPktAvgSize[row][col];
				}
			}
		}		
		
		//// GET THE EXPECTED INPUT TUPLE RATE FOR EACH COMPONENT
		compNum = t.getComponentNum();
		compNames = t.getComponents();
		compExpectedInputRate = new double[compNum];
		compActualOutputRate = new double[compNum];	
		comp2CompSpeed = new double[compNum][compNum];
		comp2CompPktSize = new double[compNum][compNum];
		
		// Get the expected input rate for the fst component
		String fstCompName = compNames[0];
		List<String> fstCompNodes = assign.getComponent2Nodes().get(fstCompName);
		for(int i=0; i< fstCompNodes.size();i++){
			Node node = cluster.getNodeByNodeId(cluster.getNodeIDByAddr(fstCompNodes.get(i)));
			ReportToNimbus report = node.getReportToNimbus();
			HashMap<Integer, Double> task2StreamInput = (HashMap<Integer, Double>) report.task2StreamInput;
			for(Map.Entry<Integer, Double> entry: task2StreamInput.entrySet()){
				String compName = assign.getTask2Component().get(entry.getKey());
				if(compName.equals(fstCompName)){
					compExpectedInputRate[0] += entry.getValue();
				}
			}
		}
		
		// Get the expected input rate for the rest components (Now assume: at steady status, output tuple rate = input tuple rate)
		for (int i = 1; i < compNum; i++){
			String compName = compNames[i];
			int curCompIndex = Arrays.asList(compNames).indexOf(compName);
			List<Integer> curCompTasks =assign.getComponent2Tasks().get(compName);
			List<String> preCompNames = t.getUpStreamComponents(compName);
			for(int j = 0; j< preCompNames.size(); j++){
				String preCompName = preCompNames.get(j);
				int preCompIndex = Arrays.asList(compNames).indexOf(preCompName);
				List<Integer> preCompTasks = assign.getComponent2Tasks().get(preCompName);
				double preCompTasksOutput = 0.0;
				double preCompTasksToCurCompTasks = 0.0;
				double preCompTasksToCurCompTasksPktSize = 0.0;
				for(int k =0; k<preCompTasks.size(); k++){
					int preTaskId = preCompTasks.get(k);
					String nodeAddrStr = assign.getTask2Node().get(preTaskId);
					Node node = cluster.getNodeByNodeId(cluster.getNodeIDByAddr(nodeAddrStr));
					ReportToNimbus report = node.getReportToNimbus();
					preCompTasksOutput += report.task2Output.get(preTaskId);
					Map<Integer, Double> ToDownStreamCompTasksTupleRate = report.task2TaskTupleRate.get(preTaskId);
					for(Map.Entry<Integer, Double> entry: ToDownStreamCompTasksTupleRate.entrySet()){
						if (curCompTasks.contains(entry.getKey())){
							preCompTasksToCurCompTasks += entry.getValue();
							preCompTasksToCurCompTasksPktSize += report.task2TaskTupleAvgSize.get(preTaskId).get(entry.getKey());
						}
					}
				}
				comp2CompPktSize[preCompIndex][curCompIndex] = preCompTasksToCurCompTasksPktSize/(preCompTasks.size()*curCompTasks.size());
				comp2CompSpeed[preCompIndex][curCompIndex] = preCompTasksToCurCompTasks/preCompTasksOutput*compExpectedInputRate[preCompIndex];
				compExpectedInputRate[i] += comp2CompSpeed[preCompIndex][curCompIndex];
				compActualOutputRate[preCompIndex] = preCompTasksOutput;
			}	
		}
		
		// Get the actual output of the last component
		String lastCompName = compNames[compNum-1];
		List<String> lastCompNodes = assign.getComponent2Nodes().get(lastCompName);
		for(int i=0; i< lastCompNodes.size();i++){
			Node node = cluster.getNodeByNodeId(cluster.getNodeIDByAddr(lastCompNodes.get(i)));
			ReportToNimbus report = node.getReportToNimbus();
			HashMap<Integer, Double> task2Throughput= (HashMap<Integer, Double>) report.task2Output;
			for(Map.Entry<Integer, Double> entry: task2Throughput.entrySet())
				compActualOutputRate[compNum-1] += entry.getValue();
		}
	}
	
	public double getExecutorResource(double[] devRes, int[] devCoreNum, double[] compWorkLoad, double[] devCap, double lowBoundRatio, double highBoundRatio){
		int devNum = devRes.length;
		double[] devResEachCore = new double[devNum];
		double[] devCapEachCore = new double[devNum];
		for(int i=0; i< devNum; i++){
			devResEachCore[i] = devRes[i] / devCoreNum[i];
			devCapEachCore[i] = devCap[i] / devCoreNum[i];
		}
		
		double minDevRes = Helper.minimium(devResEachCore);
		double minDevCap = Helper.minimium(devCapEachCore);	
	    double lowBound = minDevCap * lowBoundRatio;
		double highBound = minDevCap * highBoundRatio;
		
		for (int i=0; i< compWorkLoad.length; i++){
			System.out.println("compWorkLoad"+i+":"+compWorkLoad[i]);
		}
		double minCompWorkLoad = Helper.minimium(compWorkLoad);
		
		System.out.println("minDevRes:"+minDevRes);
		System.out.println("minDevCap:"+minDevCap);
		System.out.println("lowBound:"+lowBound);
		System.out.println("highBound:"+highBound);
		System.out.println("minCompWorkLoad:"+minCompWorkLoad);
		
		
		double minFromFeedback = (minCompWorkLoad < minDevRes)? minCompWorkLoad:minDevRes;
		System.out.println("minFromFeedback:"+minFromFeedback);
 		double executorRes = (lowBound > minFromFeedback)?lowBound:minFromFeedback;
 		executorRes = (executorRes < highBound) ? executorRes: highBound;
 		System.out.println("executorRes:"+executorRes);
 		return executorRes;
	}
	
	public double getExecutorResource(int[] devCoreNum, double[] devCap){
		int devNum = devCoreNum.length;
		double[] devCapEachCore = new double[devNum];
		for(int i=0; i< devNum; i++){
			devCapEachCore[i] = devCap[i] / devCoreNum[i];
		}
		double minDevCap = Helper.minimium(devCapEachCore);
		return minDevCap;
	}

	public boolean meetRescheduleCondition(){
		boolean meetCondition = false;
		Assignment assign = cluster.getAssignmentByTopologyId(topologyId);
		
		// check the component input/output ratio
		for(int i = 0; i< compNum; i++){
			double ratioComp = compExpectedInputRate[i]/compActualOutputRate[i] ;
			if(ratioComp > COMP_THRESHOLD){
				System.out.println("Component condition meet:"+ratioComp);
				meetCondition = true;
				break;
			}
		}	
		Map<Integer, Double> taskID2Input = new HashMap<Integer, Double>();
		Map<Integer, Double> taskID2Output = new HashMap<Integer, Double>();
		Map<Integer, Double> task2ResponseTime = new HashMap<Integer,Double>();
		
		List<String> assignedNodes = new ArrayList<String>(assign.getNode2Tasks().keySet());		
		for(int i =0; i< assignedNodes.size();i++){	
			Node node = cluster.getNodeByNodeId(cluster.getNodeIDByAddr(assignedNodes.get(i)));
			ReportToNimbus report = node.getReportToNimbus();
			
			HashMap<Integer, Map<Integer, Double>> t2tTupleRate = (HashMap<Integer, Map<Integer, Double>> ) report.task2TaskTupleRate;
			for(Map.Entry<Integer, Map<Integer, Double>> entry: t2tTupleRate.entrySet()){
				Map<Integer, Double> t2InputTupleRate = entry.getValue();
				for(Map.Entry<Integer, Double> entry2: t2InputTupleRate.entrySet()){
					int taskID = entry2.getKey();
					if(!taskID2Input.containsKey(taskID)){
						taskID2Input.put(taskID, entry2.getValue());
					} else {
						double newInput = taskID2Input.get(taskID) + entry2.getValue();
						taskID2Input.put(taskID, newInput);
					}
				}
			}
			
			HashMap<Integer, Double> taskOfFstComp2Input = (HashMap<Integer, Double>) report.task2StreamInput;
			for(Map.Entry<Integer, Double> entry: taskOfFstComp2Input.entrySet()){
				taskID2Input.put(entry.getKey(),entry.getValue());
			}
			
			HashMap<Integer, Double> t2Throughput = (HashMap<Integer, Double>) report.task2Output;
			for(Map.Entry<Integer, Double> entry: t2Throughput.entrySet()){
				taskID2Output.put(entry.getKey(), entry.getValue());
			}
			
			HashMap<Integer, Double> t2Delay = (HashMap<Integer, Double>) report.task2Delay;
			for(Map.Entry<Integer, Double> entry: t2Delay.entrySet()){
				task2ResponseTime.put(entry.getKey(), entry.getValue());
			}
		}
		
		// check the task input/output ratio
		for(Map.Entry<Integer, Double> entry: taskID2Input.entrySet()){
			
			int taskID = entry.getKey();
			double input = entry.getValue();
			double output = taskID2Output.get(taskID);
			double ratioTask = input/output;
			if(ratioTask>TASK_THRESHOLD){
				meetCondition = true;
				System.out.println("Task condition meet:"+ratioTask);
				break;
			}	
		}
		
		// check the end to end delay
		double e2eDelay = 0.0;
		HashMap<String, ArrayList<Integer>> comp2Tasks =  assign.getComponent2Tasks();
		for(Map.Entry<String, ArrayList<Integer>> entry: comp2Tasks.entrySet()){
			double maxTaskDelay = 0.0;
			ArrayList<Integer> tasks = entry.getValue();
			for(int i = 0; i<tasks.size();i++){
				double delay = task2ResponseTime.get(tasks.get(i));
				if(delay > maxTaskDelay)
					maxTaskDelay = delay;
			}
			e2eDelay += maxTaskDelay;		
		}
		if(e2eDelay > E2EDELAY_THRESHOLD){
			meetCondition = true;
			System.out.println("Delay condition meet:"+ e2eDelay);
		}
		
		// leave for future:    (OldAssignment+NewEnvironment) > (BestAssignmentWithTheSameTasksAsOldAssignment+NewEnvironment)* RESCHEDULE_RATIO
		// double old_metric_with_new_environment = fbs.getMetricOfAssign(oldAssign);
		// if (old_metric_with_new_environment > RESCHEDULE_THRESHOLD * best_metric_with_same_tasks_new_environment)
		// 		meetCondition = true;
		
		return meetCondition;
	}
	
	public double[] getMetricsOfPreAllocation() {
		switch(schType) {
			case FEEDBACK_S:
				return getMetricsOfPreAllocationByGeneticSearch();
			case TRAFFIC_S:
				return getMetricsOfPreAllocationByTStormSearch();
			case RESOURCE_S:
				return getMetricsOfPreAllocationByRStormSearch();
			case RESILIENT_S:
				return getMetricsOfPreAllocationByResilientSearch();
			default:
				return null;
		}
	}

	public double[] getMetricsOfPreAllocationByGeneticSearch(){		
		FBMStormAdvancedGeneticSearch adgs = new FBMStormAdvancedGeneticSearch(d2dPropDelay, d2dTransDelay, d2dEnergyPerbit, measuredt2tOutputRate, measuredt2tPktAvgSize);
		Matrix preAllocationGraph = new Matrix(preAllocation);
		double [] metrics = new double[2];
		metrics[0] = adgs.calculateDelay(preAllocationGraph); 	// delay metric
		metrics[1] = adgs.calculateEnergy(preAllocationGraph);	// energy metric
		return metrics;
	}
	
	public double[] getMetricsOfPreAllocationByTStormSearch(){		
		TStormSearch tss = new TStormSearch(d2dPropDelay, d2dTransDelay, d2dEnergyPerbit, measuredt2tOutputRate, measuredt2tPktAvgSize);
		Matrix preAllocationGraph = new Matrix(preAllocation);
		double [] metrics = new double[2];
		metrics[0] = tss.calculateDelay(preAllocationGraph); 	// delay metric
		metrics[1] = tss.calculateEnergy(preAllocationGraph);	// energy metric
		return metrics;
	}
	
	public double[] getMetricsOfPreAllocationByRStormSearch(){		
		RStormSearch rss = new RStormSearch(d2dPropDelay, d2dTransDelay, d2dEnergyPerbit, measuredt2tOutputRate, measuredt2tPktAvgSize);
		Matrix preAllocationGraph = new Matrix(preAllocation);
		double [] metrics = new double[2];
		metrics[0] = rss.calculateDelay(preAllocationGraph); 	// delay metric
		metrics[1] = rss.calculateEnergy(preAllocationGraph);	// energy metric
		return metrics;
	}

	public double[] getMetricsOfPreAllocationByResilientSearch(){		
		REMStormAdvancedGeneticSearch adrs = new REMStormAdvancedGeneticSearch(measuredt2tTraffic, compParallelism, exptExecutorsOfDevice, devAvailability);
		Matrix preAllocationGraph = new Matrix(preAllocation);
		double [] metrics = new double[2];
		// TODO: WHAT METRICS TO RETURN
		return metrics;
	}
}
