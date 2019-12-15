package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.MyPair;
import com.lenss.mstorm.zookeeper.Assignment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

/**
 * Created by cmy on 8/8/19.
 */

public class MessageQueues {
	static Logger logger = Logger.getLogger("MessageQueues");
    //// DISTINCT DATA QUEUES FOR TASKS
    // data queues
    public static Map<Integer,BlockingQueue<InternodePacket>> incomingQueues = new HashMap<Integer,BlockingQueue<InternodePacket>>();
    public static Map<Integer,BlockingQueue<MyPair<String, InternodePacket>>> outgoingQueues = new HashMap<Integer,BlockingQueue<MyPair<String, InternodePacket>>>();
    // result queues
    public static ConcurrentHashMap<Integer,BlockingQueue<MyPair<String, InternodePacket>>> resultQueues = new ConcurrentHashMap<Integer,BlockingQueue<MyPair<String, InternodePacket>>>();

    // add queues for tasks
    public static void addQueuesForTask(Integer taskID){
        // add queues for data
        BlockingQueue<InternodePacket> incomingQueue = new LinkedBlockingDeque<InternodePacket>();
        BlockingQueue<MyPair<String, InternodePacket>> outgoingQueue = new LinkedBlockingDeque<MyPair<String, InternodePacket>>();
        incomingQueues.put(taskID,incomingQueue);
        outgoingQueues.put(taskID,outgoingQueue);

        // Add result queue for reporting results to mobile app
        Assignment assignment = ComputingNode.getAssignment();
        HashMap<Integer,String> task2Component = assignment.getTask2Component();
        Topology topology = ComputingNode.getTopology();
        String lastComponent = topology.getComponents()[topology.getComponentNum()-1];
        if(task2Component.get(taskID).equals(lastComponent)){
            MessageQueues.addResultQueuesForTask(taskID);
        }

        // add queues for task status
        StatusOfLocalTasks.addQueuesForTask(taskID);
    }

    // add result queues for last component tasks
    public static void addResultQueuesForTask(Integer taskID){
        BlockingQueue<MyPair<String, InternodePacket>> resultQueue = new LinkedBlockingDeque<MyPair<String, InternodePacket>>();
        resultQueues.put(taskID,resultQueue);
    }

    // Add tuple to the incoming queue to process
    public static void collect(int taskid, InternodePacket data) throws InterruptedException {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (incomingQueues.get(taskid)!=null) {
            Long entryTime = System.nanoTime();
            StatusOfLocalTasks.task2EntryTimes.get(taskid).add(entryTime);
            StatusOfLocalTasks.task2EntryTimesUpStream.get(taskid).add(entryTime);
            StatusOfLocalTasks.task2EntryTimesNimbus.get(taskid).add(entryTime);
            Assignment assignment = ComputingNode.getAssignment();
            HashMap<Integer,String> task2Component = assignment.getTask2Component();
            Topology topology = ComputingNode.getTopology();
            String fstComponent = topology.getComponents()[0];
            if(task2Component.get(taskid).equals(fstComponent)){
                StatusOfLocalTasks.task2EntryTimesForFstComp.get(taskid).add(entryTime);
            }
            incomingQueues.get(taskid).put(data);
        }
    }

    // API for user: Retrieve rx tuple from incoming queue to process
    public static InternodePacket retrieveIncomingQueue(int taskid){
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (incomingQueues.get(taskid)!=null) {
            InternodePacket incomingData = incomingQueues.get(taskid).poll();
            if(incomingData!=null) {
                Long beginProcessingTime = System.nanoTime();
                StatusOfLocalTasks.task2BeginProcessingTimes.get(taskid).add(beginProcessingTime);
            }
            return incomingData;
        }
        else
            return null;
    }

    // API for user: Add tuple to the outgoing queue for tx
    public static void emit(InternodePacket data, int taskid, String Component) throws InterruptedException {
        if (outgoingQueues.get(taskid)!=null){
        	if (StatusOfLocalTasks.task2BeginProcessingTimes.get(taskid).size()>0) {
                long timePoint = System.nanoTime();
                long beginProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskid).remove(0);
                long processingTime = timePoint - beginProcessingTime;
                StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskid).add(processingTime);
            }
        	MyPair<String, InternodePacket> outData = new MyPair<String, InternodePacket>(Component, data);
            outgoingQueues.get(taskid).put(outData);
        }
    }

    // Retrieve tuple from outgoing queue to tx
    public static MyPair<String, InternodePacket> retrieveOutgoingQueue(int taskid){
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (outgoingQueues.get(taskid)!=null)
            return outgoingQueues.get(taskid).poll();
        else
            return null;
    }

    // Add tuple back to the outgoing queue to wait for the tx channel
    public static void reQueue(int taskid, MyPair<String, InternodePacket> pair) throws InterruptedException {
        if (outgoingQueues.get(taskid)!=null)
            ((LinkedBlockingDeque<MyPair<String, InternodePacket>>) outgoingQueues.get(taskid)).putFirst(pair);
    }

    // Add processing results to result queue to send to the source
    public static void emitToResultQueue(int taskid, MyPair<String, InternodePacket> pair) throws InterruptedException {
        if (resultQueues.get(taskid)!=null)
            ((LinkedBlockingDeque<MyPair<String, InternodePacket>>) resultQueues.get(taskid)).putFirst(pair);
    }

    // API for user: Retrieve processing results from result queue to sent back to the user app
    public static MyPair<String, InternodePacket> retrieveResultQueue(int taskid){
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (resultQueues.get(taskid)!=null)
            return resultQueues.get(taskid).poll();
        else
            return null;
    }

    // Check if all tuples in MStorm has been processed
    public static boolean noMoreTupleInMStorm(){
        Iterator it =  incomingQueues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            if(((BlockingQueue) entry.getValue()).size()!=0)
                return false;
        }

        it =  outgoingQueues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            if(((BlockingQueue) entry.getValue()).size()!=0)
                return false;
        }

        it =  resultQueues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            if(((BlockingQueue) entry.getValue()).size()!=0)
                return false;
        }
        return true;
    }

    // Clear records of computing serivces
    public static void removeTaskQueues(){
        // clear all queues

        incomingQueues.clear();

        outgoingQueues.clear();

        resultQueues.clear();

        StatusOfLocalTasks.task2EntryTimesForFstComp.clear();

        StatusOfLocalTasks.task2EntryTimes.clear();

        StatusOfLocalTasks.task2BeginProcessingTimes.clear();

        StatusOfLocalTasks.task2EmitTimesUpStream.clear();

        StatusOfLocalTasks.task2ResponseTimesUpStream.clear();

        StatusOfLocalTasks.task2ProcessingTimesUpStream.clear();

        StatusOfLocalTasks.task2EmitTimesNimbus.clear();

        StatusOfLocalTasks.task2ResponseTimesNimbus.clear();

        StatusOfLocalTasks.task2taskTupleNum.clear();

        StatusOfLocalTasks.task2taskTupleSize.clear();

        logger.info("All queues removed ... ");
    }
}

