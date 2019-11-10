package com.lenss.mstorm.status;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.StatisticsCalculator;
import com.lenss.mstorm.zookeeper.Assignment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by cmy on 8/17/19.
 */

public class StatusOfLocalTasks {
    //// DISTINCT STATUS QUEUES FOR TASKS

    // entry times of stream input
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimesForFstComp = new HashMap<Integer, CopyOnWriteArrayList<Long>>();

    // entry times of tuple, will be removed when a tuple leaves
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimes = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // time beginning processing, will be removed when a tuple leaves
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2BeginProcessingTimes = new HashMap<Integer, CopyOnWriteArrayList<Long>>();

    // For UpStream use: entry times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For UpStream use: emit times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EmitTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For UpStream use: delay of tuple at task
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2ResponseTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For UpStream use: processing delay of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2ProcessingTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();

    // For Nimbus use: entry times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimesNimbus = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For Nimbus use: emit times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EmitTimesNimbus = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For Nimbus use: delay of tuple at task
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2ResponseTimesNimbus= new HashMap<Integer, CopyOnWriteArrayList<Long>>();

    // total tuple number from task to downstream tasks in a sampling period
    public static Map<Integer, ConcurrentHashMap<Integer, Integer>> task2taskTupleNum = new HashMap<Integer, ConcurrentHashMap<Integer, Integer>>();
    // total tuple size from task to downstream tasks in a sampling period
    public static Map<Integer, ConcurrentHashMap<Integer, Long>> task2taskTupleSize= new HashMap<Integer, ConcurrentHashMap<Integer, Long>>();

    // For Stream Selector: input rate
    public static Map<Integer, Double> task2InputRate = new HashMap<Integer, Double>();
    // For Stream Selector: output rate
    public static Map<Integer, Double> task2OutputRate = new HashMap<Integer, Double>();
    // For Stream Selector: processing rate
    public static Map<Integer, Double> task2ProcRate = new HashMap<Integer, Double>();

    public static void addQueuesForTask(Integer taskID){
        // add queues for status report
        CopyOnWriteArrayList<Long> entryTimes = new CopyOnWriteArrayList<Long>();
        task2EntryTimes.put(taskID,entryTimes);

        CopyOnWriteArrayList<Long> beginProcessingTimes = new CopyOnWriteArrayList<Long>();
        task2BeginProcessingTimes.put(taskID,beginProcessingTimes);

        CopyOnWriteArrayList<Long> entryTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2EntryTimesUpStream.put(taskID,entryTimesUpStream);

        CopyOnWriteArrayList<Long> emitTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2EmitTimesUpStream.put(taskID,emitTimesUpStream);

        CopyOnWriteArrayList<Long> responseTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2ResponseTimesUpStream.put(taskID,responseTimesUpStream);

        CopyOnWriteArrayList<Long> processingTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2ProcessingTimesUpStream.put(taskID,processingTimesUpStream);

        CopyOnWriteArrayList<Long> entryTimesNimbus = new CopyOnWriteArrayList<Long>();
        task2EntryTimesNimbus.put(taskID,entryTimesNimbus);

        CopyOnWriteArrayList<Long> emitTimesNimbus = new CopyOnWriteArrayList<Long>();
        task2EmitTimesNimbus.put(taskID,emitTimesNimbus);

        CopyOnWriteArrayList<Long> responseTimesNimbus = new CopyOnWriteArrayList<Long>();
        task2ResponseTimesNimbus.put(taskID,responseTimesNimbus);

        Assignment assignment = ComputingNode.getAssignment();
        String comp = assignment.getTask2Component().get(taskID);
        Topology topology = ComputingNode.getTopology();
        List<String> downstreamComps = topology.getDownStreamComponents(comp);

        if(downstreamComps.size()!=0) {
            ConcurrentHashMap<Integer, Integer> task2TupleNum = new ConcurrentHashMap<Integer, Integer>();
            ConcurrentHashMap<Integer, Long> task2TupleSize = new ConcurrentHashMap<Integer, Long>();
            for (String downstreamComp : downstreamComps) {
                List<Integer> downStreamTasks = assignment.getComponent2Tasks().get(downstreamComp);
                for (int downstreamTask : downStreamTasks) {
                    task2TupleNum.put(downstreamTask, 0);
                    task2TupleSize.put(downstreamTask, 0L);
                }
            }
            task2taskTupleNum.put(taskID, task2TupleNum);
            task2taskTupleSize.put(taskID, task2TupleSize);
        }

        // Add task2EntryTimesForFstComp to get the input rate of mobile app
        HashMap<Integer, String> task2Component = assignment.getTask2Component();
        String fstComponent = topology.getComponents()[0];
        if(task2Component.get(taskID).equals(fstComponent)){
            CopyOnWriteArrayList<Long> entryTimesFirstComp = new CopyOnWriteArrayList<Long>();
            task2EntryTimesForFstComp.put(taskID,entryTimesFirstComp);
        }

        task2InputRate.put(taskID, StatisticsCalculator.SMALL_VALUE);
        task2OutputRate.put(taskID, StatisticsCalculator.SMALL_VALUE);
        task2ProcRate.put(taskID, StatisticsCalculator.SMALL_VALUE);
    }
}
