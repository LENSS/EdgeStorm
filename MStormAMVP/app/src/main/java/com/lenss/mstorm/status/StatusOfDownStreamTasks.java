package com.lenss.mstorm.status;

import android.os.SystemClock;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.utils.StatisticsCalculator;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import edu.tamu.cse.lenss.edgeKeeper.topology.TopoGraph;
import edu.tamu.cse.lenss.edgeKeeper.topology.TopoLink;

/**
 * Created by cmy on 8/17/19.
 */

public class StatusOfDownStreamTasks {
    private static final String TAG="StatusOfDownStreamTasks";
    private static Logger logger = Logger.getLogger(TAG);

    // Status from downstream reports
    public static Map<Integer, Double> taskID2InputRate = new ConcurrentHashMap<>();          // tuple/s
    public static Map<Integer, Double> taskID2OutputRate = new ConcurrentHashMap<>();         // tuple/s
    public static Map<Integer, Double> taskID2ProcRate = new ConcurrentHashMap<>();           // tuple/s
    public static Map<Integer, Double> taskID2SojournTime = new ConcurrentHashMap<>();        // ms
    public static Map<Integer, Double> taskID2InQueueLength = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2OutQueueLength = new ConcurrentHashMap<>();

    // Status from local probing results
    public static Map<Integer, Double> taskID2LinkQuality = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2RTT = new ConcurrentHashMap<>();


    public static Map<Integer, Long> taskID2LastReportTime = new ConcurrentHashMap<>();

    private static double MOVING_AVERAGE_WEIGHT = 0.0;

    public static void collectReport(int downStreamTaskID, InternodePacket pkt){
        HashMap<String, String> simpleContent = pkt.simpleContent;

        // get status from downstream report
        double procRate = new Double(simpleContent.get("procRate"));
        double inputRate = new Double(simpleContent.get("inputRate"));
        double outputRate = new Double(simpleContent.get("outputRate"));
        double inQueueLength = new Double(simpleContent.get("inQueueLength"));
        double outQueueLength = new Double(simpleContent.get("outQueueLength"));
        double sojournTime = new Double(simpleContent.get("sojournTime"));

        // get status from probing results
        String addrOfDownStreamTask = Supervisor.newAssignment.getTask2Node().get(downStreamTaskID);
        double linkQuality = StatusReporter.getLinkQuality2Device().get(addrOfDownStreamTask);
        double rtt = StatusReporter.getRTT2Device().get(addrOfDownStreamTask);

        // update linkQuality
        if(taskID2LinkQuality.containsKey(downStreamTaskID)){
            double prevLinkQuality = taskID2LinkQuality.get(downStreamTaskID);
            taskID2LinkQuality.put(downStreamTaskID, prevLinkQuality * MOVING_AVERAGE_WEIGHT + linkQuality * (1-MOVING_AVERAGE_WEIGHT));
        } else {
            taskID2LinkQuality.put(downStreamTaskID, linkQuality);
        }

        // update rtt
        if(taskID2RTT.containsKey(downStreamTaskID)){
            double prevRtt = taskID2RTT.get(downStreamTaskID);
            taskID2RTT.put(downStreamTaskID, prevRtt * MOVING_AVERAGE_WEIGHT + rtt * (1-MOVING_AVERAGE_WEIGHT));
        } else {
            taskID2RTT.put(downStreamTaskID, rtt);
        }

        // update processing rate
        if(taskID2ProcRate.containsKey(downStreamTaskID)) {
            if(procRate > StatisticsCalculator.SMALL_VALUE) {
                double prevProcRate = taskID2ProcRate.get(downStreamTaskID);
                taskID2ProcRate.put(downStreamTaskID, prevProcRate * MOVING_AVERAGE_WEIGHT + procRate * (1-MOVING_AVERAGE_WEIGHT));
            }
        } else {
            taskID2ProcRate.put(downStreamTaskID, procRate);
        }

        // update input rate
        if(taskID2InputRate.containsKey(downStreamTaskID)) {
            if(inputRate > StatisticsCalculator.SMALL_VALUE){
                double prevInputRate = taskID2InputRate.get(downStreamTaskID);
                taskID2InputRate.put(downStreamTaskID, prevInputRate * MOVING_AVERAGE_WEIGHT + inputRate * (1-MOVING_AVERAGE_WEIGHT));
            }
        } else {
            taskID2InputRate.put(downStreamTaskID, inputRate);
        }

        // update output rate
        if(taskID2OutputRate.containsKey(downStreamTaskID)) {
            if(outputRate > StatisticsCalculator.SMALL_VALUE){
                double prevOutputRate = taskID2OutputRate.get(downStreamTaskID);
                taskID2OutputRate.put(downStreamTaskID, prevOutputRate * MOVING_AVERAGE_WEIGHT + outputRate * (1-MOVING_AVERAGE_WEIGHT));
            }
        } else {
            taskID2OutputRate.put(downStreamTaskID, outputRate);
        }

        // update input queue length
        if(taskID2InQueueLength.containsKey(downStreamTaskID)) {
            double prevInQueueLength = taskID2InQueueLength.get(downStreamTaskID);
            taskID2InQueueLength.put(downStreamTaskID, prevInQueueLength * MOVING_AVERAGE_WEIGHT + inQueueLength * (1-MOVING_AVERAGE_WEIGHT));
        } else {
            taskID2InQueueLength.put(downStreamTaskID, inQueueLength);
        }

        // update output queue length
        if(taskID2OutQueueLength.containsKey(downStreamTaskID)) {
            double prevOutQueueLength = taskID2OutQueueLength.get(downStreamTaskID);
            taskID2OutQueueLength.put(downStreamTaskID, prevOutQueueLength * MOVING_AVERAGE_WEIGHT + outQueueLength * (1-MOVING_AVERAGE_WEIGHT));
        } else {
            taskID2OutQueueLength.put(downStreamTaskID, outQueueLength);
        }

        // update sojourn time
        if(taskID2SojournTime.containsKey(downStreamTaskID)) {
            if(sojournTime < StatisticsCalculator.LARGE_VALUE) {
                double prevSojournTime = taskID2SojournTime.get(downStreamTaskID);
                taskID2SojournTime.put(downStreamTaskID, prevSojournTime * MOVING_AVERAGE_WEIGHT + sojournTime * (1-MOVING_AVERAGE_WEIGHT));
            }
        } else {
            taskID2SojournTime.put(downStreamTaskID, sojournTime);
        }


        // get the time getting this report packet
        Long reportTime = SystemClock.elapsedRealtimeNanos();
        taskID2LastReportTime.put(downStreamTaskID, reportTime);
    }

    public static void collectAppStatusOfDownStreamTasks(Set<Integer> downStreamTasksAll, ReportToUpstreamWithStatusOfLocalTasks reportFromDownStreamNode){
        for(Map.Entry<Integer, Double> entry: reportFromDownStreamNode.task2InputRate.entrySet()){
            if(downStreamTasksAll.contains(entry.getKey())){
                taskID2InputRate.put(entry.getKey(),entry.getValue());
            }
        }

        for(Map.Entry<Integer, Double> entry: reportFromDownStreamNode.task2OutputRate.entrySet()){
            if(downStreamTasksAll.contains(entry.getKey())){
                taskID2OutputRate.put(entry.getKey(),entry.getValue());
            }
        }

        for(Map.Entry<Integer, Double> entry: reportFromDownStreamNode.task2ProcRate.entrySet()){
            if(downStreamTasksAll.contains(entry.getKey())){
                taskID2ProcRate.put(entry.getKey(),entry.getValue());
            }
        }

        for(Map.Entry<Integer, Double> entry: reportFromDownStreamNode.task2SojournTime.entrySet()){
            if(downStreamTasksAll.contains(entry.getKey())){
                taskID2SojournTime.put(entry.getKey(),entry.getValue());
            }
        }

        for(Map.Entry<Integer, Double> entry: reportFromDownStreamNode.task2InQueueLength.entrySet()){
            if(downStreamTasksAll.contains(entry.getKey())){
                taskID2InQueueLength.put(entry.getKey(),entry.getValue());
            }
        }

        for(Map.Entry<Integer, Double> entry: reportFromDownStreamNode.task2OutQueueLength.entrySet()){
            if(downStreamTasksAll.contains(entry.getKey())){
                taskID2OutQueueLength.put(entry.getKey(),entry.getValue());
            }
        }
    }

    public static void collectNetStatusOfDownStreamTasks(Set<Integer> downStreamTasksAll, TopoGraph networkInfo){
        Assignment assign = ComputingNode.getAssignment();
        Map<Integer, String> task2Node = assign.getTask2Node();

        for(Integer downStreamTask: downStreamTasksAll){
            String downStreamNode = task2Node.get(downStreamTask);
            TopoLink link = networkInfo.getEdge(networkInfo.ownNode, networkInfo.getVertexByGuid(downStreamNode));
            if(link!=null){
                double linkQuality = 1.0/link.getEtx();
                taskID2LinkQuality.put(downStreamTask, linkQuality);
                if(link.rtt!=null) {
                    double rtt = link.rtt;
                    taskID2RTT.put(downStreamTask, rtt);
                }
            } else {
                logger.info("No link found from " + networkInfo.ownNode.guid + " to " + downStreamNode);
            }
        }
    }

    public static void removeReport(int downStreamTaskID){
        taskID2RTT.remove(downStreamTaskID);
        taskID2LinkQuality.remove(downStreamTaskID);
        taskID2ProcRate.remove(downStreamTaskID);
        taskID2InputRate.remove(downStreamTaskID);
        taskID2OutputRate.remove(downStreamTaskID);
        taskID2InQueueLength.remove(downStreamTaskID);
        taskID2OutQueueLength.remove(downStreamTaskID);
        taskID2SojournTime.remove(downStreamTaskID);
        taskID2LastReportTime.remove(downStreamTaskID);
    }

    public static void removeAllStatus(){
        taskID2RTT.clear();
        taskID2LinkQuality.clear();
        taskID2ProcRate.clear();
        taskID2InputRate.clear();
        taskID2OutputRate.clear();
        taskID2InQueueLength.clear();
        taskID2OutQueueLength.clear();
        taskID2SojournTime.clear();
        taskID2LastReportTime.clear();
    }

    public static void updateDownStreamTaskLink(int downStreamTaskID){
        double preLinkQuality = taskID2LinkQuality.get(downStreamTaskID);
        taskID2LinkQuality.put(downStreamTaskID, preLinkQuality/2);
        double preRTT = taskID2RTT.get(downStreamTaskID);
        taskID2RTT.put(downStreamTaskID, preRTT/2);

    }

    public static void initDownStreamTaskLink(int downStreamTaskID){
        taskID2LinkQuality.put(downStreamTaskID, 1.0);
        taskID2RTT.put(downStreamTaskID, 100.0);
    }
}
