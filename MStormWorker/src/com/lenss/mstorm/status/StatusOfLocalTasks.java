package com.lenss.mstorm.status;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.topology.Topology;
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
    // entry times of input
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimesForFstComp = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // entry times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EntryTimes = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // time beginning processing
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2BeginProcessingTimes = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For UpStream use: emit times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EmitTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For UpStream use: delay of tuple at task
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2ResponseTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For UpStream use: processing delay of tuple at task
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2ProcessingTimesUpStream = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For Nimbus use: emit times of tuple
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2EmitTimesNimbus = new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // For Nimbus use: delay of tuple at task
    public static Map<Integer, CopyOnWriteArrayList<Long>> task2ResponseTimesNimbus= new HashMap<Integer, CopyOnWriteArrayList<Long>>();
    // total tuple number from task to downstream tasks in a sampling period
    public static Map<Integer, ConcurrentHashMap<Integer, Integer>> task2taskTupleNum = new HashMap<Integer, ConcurrentHashMap<Integer, Integer>>();
    // total tuple size from task to downstream tasks in a sampling period
    public static Map<Integer, ConcurrentHashMap<Integer, Long>> task2taskTupleSize= new HashMap<Integer, ConcurrentHashMap<Integer, Long>>();

    public static void addQueuesForTask(Integer taskID){
        // add queues for status report
        CopyOnWriteArrayList<Long> entryTimes = new CopyOnWriteArrayList<Long>();
        task2EntryTimes.put(taskID,entryTimes);

        CopyOnWriteArrayList<Long> beginProcessingTimes = new CopyOnWriteArrayList<Long>();
        task2BeginProcessingTimes.put(taskID,beginProcessingTimes);

        CopyOnWriteArrayList<Long> emitTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2EmitTimesUpStream.put(taskID,emitTimesUpStream);

        CopyOnWriteArrayList<Long> responseTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2ResponseTimesUpStream.put(taskID,responseTimesUpStream);

        CopyOnWriteArrayList<Long> processingTimesUpStream = new CopyOnWriteArrayList<Long>();
        task2ProcessingTimesUpStream.put(taskID,processingTimesUpStream);

        CopyOnWriteArrayList<Long> emitTimesNimbus = new CopyOnWriteArrayList<Long>();
        task2EmitTimesNimbus.put(taskID,emitTimesNimbus);

        CopyOnWriteArrayList<Long> responseTimesNimbus = new CopyOnWriteArrayList<Long>();
        task2ResponseTimesNimbus.put(taskID,responseTimesNimbus);

        ConcurrentHashMap<Integer, Integer> task2TupleNum = new ConcurrentHashMap<Integer, Integer>();
        ConcurrentHashMap<Integer, Long> task2TupleSize = new ConcurrentHashMap<Integer, Long>();

        Assignment assignment = ComputingNode.getAssignment();
        String comp = assignment.getTask2Component().get(taskID);
        Topology topology = ComputingNode.getTopology();
        List<String> downstreamComps = topology.getDownStreamComponents(comp);
        for(String downstreamComp: downstreamComps){
            List<Integer> downStreamTasks = assignment.getComponent2Tasks().get(downstreamComp);
            for(int downstreamTask: downStreamTasks){
                task2TupleNum.put(downstreamTask, 0);
                task2TupleSize.put(downstreamTask, 0L);
            }
        }
        task2taskTupleNum.put(taskID,task2TupleNum);
        task2taskTupleSize.put(taskID,task2TupleSize);

        // Add task2EntryTimesForFstComp to get the input rate of mobile app
        HashMap<Integer, String> task2Component = assignment.getTask2Component();
        String fstComponent = topology.getComponents()[0];
        if(task2Component.get(taskID).equals(fstComponent)){
            StatusOfLocalTasks.addTask2EntryTimesForFstComp(taskID);
        }

    }

    // add stream input queues for mobile app at the first component tasks
    public static void addTask2EntryTimesForFstComp(Integer taskID){
        CopyOnWriteArrayList<Long> entryTimes = new CopyOnWriteArrayList<Long>();
        task2EntryTimesForFstComp.put(taskID,entryTimes);
    }
}
