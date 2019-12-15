package com.lenss.mstorm.communication.internodes;

import android.os.AsyncTask;
import android.os.SystemClock;
import android.util.Pair;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by cmy on 8/8/19.
 */

public class MessageQueues {

    /// LOGGER
    private static final String TAG="MessageQueues";
    private static Logger logger = Logger.getLogger(TAG);

    //// DISTINCT DATA QUEUES FOR TASKS

    // data queues
    public static Map<Integer,BlockingQueue<InternodePacket>> incomingQueues = new HashMap<Integer,BlockingQueue<InternodePacket>>();
    public static Map<Integer,BlockingQueue<Pair<String, InternodePacket>>> outgoingQueues = new HashMap<Integer,BlockingQueue<Pair<String, InternodePacket>>>();

    // result queues
    public static ConcurrentHashMap<Integer,BlockingQueue<Pair<String, InternodePacket>>> resultQueues = new ConcurrentHashMap<Integer,BlockingQueue<Pair<String, InternodePacket>>>();

    // add queues for tasks
    public static void addQueuesForTask(Integer taskID){
        // add queues for data
        BlockingQueue<InternodePacket> incomingQueue = new LinkedBlockingDeque<InternodePacket>();
        BlockingQueue<Pair<String, InternodePacket>> outgoingQueue = new LinkedBlockingDeque<Pair<String, InternodePacket>>();
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
        StatusOfLocalTasks.addStatusQueuesForTask(taskID);
    }

    // add result queues for last component tasks
    public static void addResultQueuesForTask(Integer taskID){
        BlockingQueue<Pair<String, InternodePacket>> resultQueue = new LinkedBlockingDeque<Pair<String, InternodePacket>>();
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
            long entryTime = SystemClock.elapsedRealtimeNanos();
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
                long beginProcessingTime = SystemClock.elapsedRealtimeNanos();
                StatusOfLocalTasks.task2BeginProcessingTimes.get(taskid).add(beginProcessingTime);
            }
            return incomingData;
        } else {
            return null;
        }
    }

    // API for user: Add tuple to the outgoing queue for tx
    public static void emit(InternodePacket data, int taskid, String Component) throws InterruptedException {
        if (outgoingQueues.get(taskid)!=null){
            if (StatusOfLocalTasks.task2BeginProcessingTimes.get(taskid).size()>0) {
                long timePoint = SystemClock.elapsedRealtimeNanos();
                long beginProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskid).remove(0);
                long processingTime = timePoint - beginProcessingTime;
                StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskid).add(processingTime);
            }
            Pair<String, InternodePacket> outData = new Pair<String, InternodePacket>(Component, data);
            outgoingQueues.get(taskid).put(outData);
        }
    }

    // Retrieve tuple from outgoing queue to tx
    public static Pair<String, InternodePacket> retrieveOutgoingQueue(int taskid){
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
    public static void reQueue(int taskid, Pair<String, InternodePacket> pair) throws InterruptedException {
        if (outgoingQueues.get(taskid)!=null)
            ((LinkedBlockingDeque<Pair<String, InternodePacket>>) outgoingQueues.get(taskid)).putFirst(pair);
    }

    // Add processing results to result queue to send to the source
    public static void emitToResultQueue(int taskid, Pair<String, InternodePacket> pair) throws InterruptedException {
        if (resultQueues.get(taskid)!=null)
            ((LinkedBlockingDeque<Pair<String, InternodePacket>>) resultQueues.get(taskid)).putFirst(pair);
    }

    // API for user: Retrieve processing results from result queue to sent back to the user app
    public static Pair<String, InternodePacket> retrieveResultQueue(int taskid){
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

        StatusOfLocalTasks.removeStatusQueues();

        logger.info("All task queues removed!");
    }
}
