package com.lenss.mstorm.status;

import android.os.SystemClock;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.utils.StatisticsCalculator;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by cmy on 8/17/19.
 */

public class StatusOfDownStreamTasks {
    // Status from downstream reports
    public static Map<Integer, Double> taskID2ProcRate = new ConcurrentHashMap<>();           // tuple/s
    public static Map<Integer, Double> taskID2InputRate = new ConcurrentHashMap<>();          // tuple/s
    public static Map<Integer, Double> taskID2OutputRate = new ConcurrentHashMap<>();         // tuple/s
    public static Map<Integer, Double> taskID2InQueueLength = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2OutQueueLength = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2RSSI = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2SojournTime = new ConcurrentHashMap<>();        // ms

    // Status from local probing results
    public static Map<Integer, Double> taskID2LinkQuality = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2RTT = new ConcurrentHashMap<>();


    public static Map<Integer, Long> taskID2LastReportTime = new ConcurrentHashMap<>();

    private static double MOVING_AVERAGE_WEIGHT = 0.0;

    public static void collectReport(int downStreamTaskID, InternodePacket pkt){
        HashMap<String, String> simpleContent = pkt.simpleContent;

        // get status from downstream reports
        double procRate = new Double(simpleContent.get("procRate"));
        double inputRate = new Double(simpleContent.get("inputRate"));
        double outputRate = new Double(simpleContent.get("outputRate"));
        double inQueueLength = new Double(simpleContent.get("inQueueLength"));
        double outQueueLength = new Double(simpleContent.get("outQueueLength"));
        double sojournTime = new Double(simpleContent.get("sojournTime"));
        double rssi = new Double(simpleContent.get("rssi"));

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

        // update rssi
        if(taskID2RSSI.containsKey(downStreamTaskID)){
            double preRssi = taskID2RSSI.get(downStreamTaskID);
            taskID2RSSI.put(downStreamTaskID, preRssi * MOVING_AVERAGE_WEIGHT + rssi * (1-MOVING_AVERAGE_WEIGHT));
        } else {
            taskID2RSSI.put(downStreamTaskID, rssi);
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

    public static void removeReport(int downStreamTaskID){
        taskID2RSSI.remove(downStreamTaskID);
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
        taskID2RSSI.clear();
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

    public static void setDownStreamTaskDisconnected(int downStreamTaskID){
        taskID2LinkQuality.put(downStreamTaskID, StatisticsCalculator.SMALL_VALUE);
        taskID2RTT.put(downStreamTaskID, StatisticsCalculator.LARGE_VALUE);

    }

    public static void setDownStreamTaskConnected(int downStreamTaskID){
        taskID2LinkQuality.put(downStreamTaskID, 1.0);
        taskID2RTT.put(downStreamTaskID, 100.0);

    }
}
