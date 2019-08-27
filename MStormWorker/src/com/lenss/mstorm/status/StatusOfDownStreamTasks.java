package com.lenss.mstorm.status;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.zookeeper.Assignment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by cmy on 8/17/19.
 */

public class StatusOfDownStreamTasks {
    // Fine grained status
    public static Map<Integer, Double> taskID2ProcRate = new ConcurrentHashMap<>();           // tuple/s
    public static Map<Integer, Double> taskID2InputRate = new ConcurrentHashMap<>();          // tuple/s
    public static Map<Integer, Double> taskID2OutputRate = new ConcurrentHashMap<>();         // tuple/s
    public static Map<Integer, Double> taskID2InQueueLength = new ConcurrentHashMap<>();
    public static Map<Integer, Double> taskID2OutQueueLength = new ConcurrentHashMap<>();

    // Coarse grained status
    public static Map<Integer, Double> taskID2SojournTime = new ConcurrentHashMap<>();        // ms

    public static Map<Integer, Long> taskID2LastReportTime = new ConcurrentHashMap<>();

    private static double MOVING_AVERAGE_WEIGHT = 0.2;

    public static void collectReport(int downStreamTaskID, InternodePacket pkt){
        HashMap<String, String> simpleContent = pkt.simpleContent;

        // get status from report packet
        double procRate = new Double(simpleContent.get("procRate"));
        double inputRate = new Double(simpleContent.get("inputRate"));
        double outputRate = new Double(simpleContent.get("outputRate"));
        double inQueueLength = new Double(simpleContent.get("inQueueLength"));
        double outQueueLength = new Double(simpleContent.get("outQueueLength"));
        double sojournTime = new Double(simpleContent.get("sojournTime"));

        // update processing rate
        double prevProcRate = 0.0;
        if(taskID2ProcRate.containsKey(downStreamTaskID)) {
            prevProcRate = taskID2ProcRate.get(downStreamTaskID);
        }
        taskID2ProcRate.put(downStreamTaskID, prevProcRate * MOVING_AVERAGE_WEIGHT + procRate * (1-MOVING_AVERAGE_WEIGHT));

        // update input rate
        double prevInputRate = 0.0;
        if(taskID2InputRate.containsKey(downStreamTaskID)) {
            prevInputRate = taskID2InputRate.get(downStreamTaskID);
        }
        taskID2InputRate.put(downStreamTaskID, prevInputRate * MOVING_AVERAGE_WEIGHT + inputRate * (1-MOVING_AVERAGE_WEIGHT));

        // update output rate
        double prevOutputRate = 0.0;
        if(taskID2OutputRate.containsKey(downStreamTaskID)) {
            prevOutputRate = taskID2OutputRate.get(downStreamTaskID);
        }
        taskID2OutputRate.put(downStreamTaskID, prevOutputRate * MOVING_AVERAGE_WEIGHT + outputRate * (1-MOVING_AVERAGE_WEIGHT));

        // update input queue length
        double prevInQueueLength = 0.0;
        if(taskID2InQueueLength.containsKey(downStreamTaskID)) {
            prevInQueueLength = taskID2InQueueLength.get(downStreamTaskID);
        }
        taskID2InQueueLength.put(downStreamTaskID, prevInQueueLength * MOVING_AVERAGE_WEIGHT + inQueueLength * (1-MOVING_AVERAGE_WEIGHT));

        // update output queue rate
        double prevOutQueueLength = 0.0;
        if(taskID2OutQueueLength.containsKey(downStreamTaskID)) {
            prevOutQueueLength = taskID2OutQueueLength.get(downStreamTaskID);
        }
        taskID2OutQueueLength.put(downStreamTaskID, prevOutQueueLength * MOVING_AVERAGE_WEIGHT + outQueueLength * (1-MOVING_AVERAGE_WEIGHT));

        // update sojourn time
        double prevSojournTime = 0.0;
        if(taskID2SojournTime.containsKey(downStreamTaskID)) {
            prevSojournTime = taskID2SojournTime.get(downStreamTaskID);
        }
        taskID2SojournTime.put(downStreamTaskID, prevSojournTime * MOVING_AVERAGE_WEIGHT + sojournTime * (1-MOVING_AVERAGE_WEIGHT));

        // get the time getting this report packet
        Long reportTime = System.nanoTime();
        taskID2LastReportTime.put(downStreamTaskID, reportTime);
    }

    public static void removeReport(int downStreamTaskID){
        taskID2ProcRate.remove(downStreamTaskID);
        taskID2InputRate.remove(downStreamTaskID);
        taskID2OutputRate.remove(downStreamTaskID);
        taskID2InQueueLength.remove(downStreamTaskID);
        taskID2OutQueueLength.remove(downStreamTaskID);
        taskID2SojournTime.remove(downStreamTaskID);
        taskID2LastReportTime.remove(downStreamTaskID);
    }

    public static void removeAllStatus(){
        taskID2ProcRate.clear();
        taskID2InputRate.clear();
        taskID2OutputRate.clear();
        taskID2InQueueLength.clear();
        taskID2OutQueueLength.clear();
        taskID2SojournTime.clear();
        taskID2LastReportTime.clear();
    }
}
