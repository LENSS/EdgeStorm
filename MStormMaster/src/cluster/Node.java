package cluster;


import java.security.acl.LastOwnerException;
import java.util.Map;

import status.ReportToNimbus;


public class Node {
	private String address;
	private ReportToNimbus latestPhoneStatus;
	private boolean hasUpdatedStatusReport;
	private int publicOrPrivate;
	private int mobileOrServer;
	private double availability;
	
	private static final double RATIO = 0.37;   // 1/e
	public static final int PUBLIC = 1;
	public static final int PRIVATE = 0;
	
	public Node(String addr, int pubOrPri, int mobOrSer, double availability)
	{
		address = addr;
		publicOrPrivate = pubOrPri;
		mobileOrServer = mobOrSer;
		this.availability = availability;
		latestPhoneStatus = null;
		hasUpdatedStatusReport = false;
	}
	
	public String getAddress(){
		return address;
	}
	
	public int getAddrStatus() {
		return publicOrPrivate;
	}
	
	public int getNodeType() {
		return mobileOrServer;
	}
	
	public double getNodeAvailibility() {
		return availability;
	}
	
	public Boolean isReportContainingTaskInfor(){
		if(latestPhoneStatus!=null)
			return (latestPhoneStatus.isIncludingTaskReport);
		else
			return false;
	}
	
	public Boolean isStatusReportUpdated(){
		return hasUpdatedStatusReport;
	}
	
	public void removePhoneStatus(){
		latestPhoneStatus = null;
	}
	
	public void updateStatus(ReportToNimbus newStatus){
		if(latestPhoneStatus==null){
			latestPhoneStatus = newStatus;
		} else{
			if(latestPhoneStatus.isIncludingTaskReport && newStatus.isIncludingTaskReport)
				update(latestPhoneStatus,newStatus,RATIO);
			else
				latestPhoneStatus = newStatus;
		}
		hasUpdatedStatusReport = true;
	}
	
	public void hasBeenRescheduledWithUpdatedReport(){
		hasUpdatedStatusReport = false;
	}
	
	public ReportToNimbus update(ReportToNimbus latestPhoneStatus, ReportToNimbus newPhoneStatus, double ratio){
		//// update CPU related parameters
		latestPhoneStatus.availability = newPhoneStatus.availability;
		latestPhoneStatus.cpuCoreNum = newPhoneStatus.cpuCoreNum;
		latestPhoneStatus.cpuFrequency = latestPhoneStatus.cpuFrequency * ratio + newPhoneStatus.cpuFrequency * (1-ratio);
		latestPhoneStatus.cpuUsage = latestPhoneStatus.cpuUsage * ratio + newPhoneStatus.cpuUsage * (1-ratio);
		latestPhoneStatus.availCPUForMStormTasks = latestPhoneStatus.availCPUForMStormTasks * ratio + newPhoneStatus.availCPUForMStormTasks * (1-ratio);
		// update the CPU usage of each task thread
		for(Map.Entry<Integer, Double> entry: latestPhoneStatus.taskID2CPUUsage.entrySet()){
			Integer taskID = entry.getKey();
			double cpuUsage = entry.getValue() * ratio + newPhoneStatus.taskID2CPUUsage.get(taskID) * (1-ratio);
			entry.setValue(cpuUsage);
		}
		
		//// update memory related parameters
		latestPhoneStatus.availableMemory = latestPhoneStatus.availableMemory * ratio + newPhoneStatus.availableMemory * (1-ratio);
		
		//// update network related parameters
		latestPhoneStatus.txBandwidth = latestPhoneStatus.txBandwidth * ratio + newPhoneStatus.availableMemory * (1-ratio);
		latestPhoneStatus.rxBandwidth = latestPhoneStatus.rxBandwidth * ratio + newPhoneStatus.rxBandwidth * (1-ratio);
		// update the rtt to each other nodes
		for(Map.Entry<String,Double> entry: latestPhoneStatus.rttMap.entrySet()){
			String nodeAddr = entry.getKey();
			double rtt = entry.getValue() * ratio + newPhoneStatus.rttMap.get(nodeAddr)*(1-ratio);
			entry.setValue(rtt);
		}
		latestPhoneStatus.wifiLinkSpeed = latestPhoneStatus.wifiLinkSpeed * ratio + newPhoneStatus.wifiLinkSpeed*(1-ratio);
		latestPhoneStatus.rSSI = latestPhoneStatus.rSSI * ratio + newPhoneStatus.rSSI*(1-ratio);
		
		//// update energy related parameters
		latestPhoneStatus.batteryCapacity = latestPhoneStatus.batteryCapacity * ratio + newPhoneStatus.batteryCapacity * (1-ratio);
		latestPhoneStatus.batteryLevel = latestPhoneStatus.batteryLevel * ratio + newPhoneStatus.batteryLevel * (1-ratio);
		latestPhoneStatus.energyPerBitRx = latestPhoneStatus.energyPerBitRx * ratio + newPhoneStatus.energyPerBitRx * (1-ratio);
		latestPhoneStatus.energyPerBitTx = latestPhoneStatus.energyPerBitTx * ratio + newPhoneStatus.energyPerBitTx * (1-ratio);
		
		//if (latestPhoneStatus.isIncludingTaskReport == true && newPhoneStatus.isIncludingTaskReport == true){
			//// update task statistics
		for(Map.Entry<Integer, Map<Integer, Double>> entry1: latestPhoneStatus.task2TaskTupleRate.entrySet()){
			Integer taskID = entry1.getKey();
			for(Map.Entry<Integer, Double> entry2: entry1.getValue().entrySet()){
				int remoteTaskID = entry2.getKey();
				double tupleRate = entry2.getValue()*ratio+newPhoneStatus.task2TaskTupleRate.get(taskID).get(remoteTaskID)*(1-ratio);
				entry2.setValue(tupleRate);
			}
		}			
		for(Map.Entry<Integer, Map<Integer, Double>> entry1: latestPhoneStatus.task2TaskTupleAvgSize.entrySet()){
			Integer taskID = entry1.getKey();
			for(Map.Entry<Integer, Double> entry2: entry1.getValue().entrySet()){
				int remoteTaskID = entry2.getKey();
				double tupleRate = entry2.getValue()*ratio+newPhoneStatus.task2TaskTupleAvgSize.get(taskID).get(remoteTaskID)*(1-ratio);
				entry2.setValue(tupleRate);
			}
		}
		for(Map.Entry<Integer, Double> entry:latestPhoneStatus.task2Output.entrySet()){
			int taskID = entry.getKey();
			double throughput = entry.getValue()*ratio+newPhoneStatus.task2Output.get(taskID)*(1-ratio);
			entry.setValue(throughput);
		}
		for(Map.Entry<Integer, Double> entry:latestPhoneStatus.task2Delay.entrySet()){
			int taskID = entry.getKey();
			double delay = entry.getValue()*ratio+newPhoneStatus.task2Delay.get(taskID)*(1-ratio);
			entry.setValue(delay);
		}
/*		} else {
			latestPhoneStatus.isIncludingTaskReport = newPhoneStatus.isIncludingTaskReport;
			latestPhoneStatus.task2TaskTupleRate = newPhoneStatus.task2TaskTupleRate;
			latestPhoneStatus.task2TaskTupleAvgSize = newPhoneStatus.task2TaskTupleAvgSize;
			latestPhoneStatus.task2Throughput = newPhoneStatus.task2Throughput;
			latestPhoneStatus.task2Delay = newPhoneStatus.task2Delay;
		}*/
		
		return latestPhoneStatus;
	}

	public ReportToNimbus getReportToNimbus(){
		return latestPhoneStatus;
	}
	
}
