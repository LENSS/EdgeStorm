package com.lenss.mstorm.communication.internodes;

import android.os.SystemClock;
import android.util.Pair;

import com.google.gson.Gson;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.status.StatusOfDownStreamTasks;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.HashMap;

public class Dispatcher implements Runnable {
    private boolean finished = false;

    @Override
    public void run() {
        Assignment assignment = ComputingNode.getAssignment();
        String serTopology = assignment.getSerTopology();
        Topology topology = new Gson().fromJson(serTopology, Topology.class);
        HashMap<String, Integer> grouping = topology.getGroupings();
        ArrayList<Integer> localTasks = assignment.getNode2Tasks().get(MStorm.GUID);

        while (!Thread.currentThread().isInterrupted() && !finished) {
            if(localTasks!=null) {
                for (int taskID: localTasks) {
                    Pair<String, InternodePacket> outdata = MessageQueues.retrieveOutgoingQueue(taskID);
                    if (outdata != null) {
                        StatusReporter.getInstance().updateIsIncludingTask();
                        if (!outdata.first.equals("END")) { // Go to tasks of the next component
                            if ((outdata.second != null)) {
                                Status status;
                                if (grouping.get(outdata.first) == Topology.Shuffle) {         // Shuffle stream grouping
                                    status = ChannelManager.sendToRandomDownstreamTask(outdata.first, outdata.second);
                                    if (!status.success) {
                                        try {
                                            MessageQueues.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                    }
                                } else if (grouping.get(outdata.first) == Topology.MinSojournTime) {
                                    status = ChannelManager.sendToDownstreamTaskMinSojournTime(outdata.first, outdata.second, false);
                                    if (!status.success) {
                                        if(status.remoteTaskID!=-1){
                                            updateDownStreamTaskStatusOnFailure(status.remoteTaskID);
                                        }
                                        status = ChannelManager.sendToDownstreamTaskMinSojournTime(outdata.first, outdata.second, true);
                                        if (!status.success) {
                                            if(status.remoteTaskID!=-1){
                                                updateDownStreamTaskStatusOnFailure(status.remoteTaskID);
                                            }
                                            try {
                                                MessageQueues.reQueue(taskID, outdata);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                            updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                    }
                                } else if(grouping.get(outdata.first) == Topology.SojournTimeProb){
                                    status = ChannelManager.sendToDownstreamTaskSojournTimeProb(outdata.first, outdata.second, false);
                                    if (!status.success) {
                                        if(status.remoteTaskID!=-1){
                                            updateDownStreamTaskStatusOnFailure(status.remoteTaskID);
                                        }
                                        status = ChannelManager.sendToDownstreamTaskSojournTimeProb(outdata.first, outdata.second, true);
                                        if (!status.success) {
                                            if(status.remoteTaskID!=-1){
                                                updateDownStreamTaskStatusOnFailure(status.remoteTaskID);
                                            }
                                            try {
                                                MessageQueues.reQueue(taskID, outdata);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                            updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                    }
                                } else if (grouping.get(outdata.first) == Topology.MinEWT){
                                    status = ChannelManager.sendToDownstreamTaskMinEWT(outdata.first, outdata.second, false);
                                    if (!status.success) {
                                        if(status.remoteTaskID!=-1){
                                            updateDownStreamTaskStatusOnFailure(status.remoteTaskID);
                                        }
                                        status = ChannelManager.sendToDownstreamTaskMinEWT(outdata.first, outdata.second, true);
                                        if (!status.success) {
                                            if(status.remoteTaskID!=-1){
                                                updateDownStreamTaskStatusOnFailure(status.remoteTaskID);
                                            }
                                            try {
                                                MessageQueues.reQueue(taskID, outdata);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                            updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, status.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(status.remoteTaskID);
                                    }
                                }
                            }
                        } else { // Go to result queue
                            try {
                                MessageQueues.emitToResultQueue(taskID, outdata);

                                long timePoint = SystemClock.elapsedRealtimeNanos();
                                StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(timePoint);
                                StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(timePoint);

                                if(StatusOfLocalTasks.task2EntryTimes.get(taskID).size()>0) {
                                    long entryTimePoint = StatusOfLocalTasks.task2EntryTimes.get(taskID).remove(0);
                                    long responseTime = timePoint - entryTimePoint;
                                    StatusOfLocalTasks.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                                    StatusOfLocalTasks.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            } else{
                finished = true;
            }
        }
        System.out.println("The Dispatcher thread has stopped ... ");
    }

    public void updateLocalTaskStatus(int taskID, int remoteTaskID, long size){
        long timePoint = SystemClock.elapsedRealtimeNanos();
        StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(timePoint);
        StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(timePoint);

        if(StatusOfLocalTasks.task2EntryTimes.get(taskID).size()>0) {
            long entryTimePoint = StatusOfLocalTasks.task2EntryTimes.get(taskID).remove(0);
            long responseTime = timePoint - entryTimePoint;
            StatusOfLocalTasks.task2ResponseTimesUpStream.get(taskID).add(responseTime);
            StatusOfLocalTasks.task2ResponseTimesNimbus.get(taskID).add(responseTime);
        }

        int newTotalTupleNum = StatusOfLocalTasks.task2taskTupleNum.get(taskID).get(remoteTaskID) + 1;
        StatusOfLocalTasks.task2taskTupleNum.get(taskID).put(remoteTaskID, newTotalTupleNum);

        long newTotalTupleSize = StatusOfLocalTasks.task2taskTupleSize.get(taskID).get(remoteTaskID) + size;
        StatusOfLocalTasks.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
    }

    public void updateDownStreamTaskStatusOnSuccess(int remoteTaskID){
        if(StatusOfDownStreamTasks.taskID2InQueueLength.containsKey(remoteTaskID)){
            //double procRate = StatusOfDownStreamTasks.taskID2ProcRate.get(remoteTaskID);
            //double inputRate = StatusOfDownStreamTasks.taskID2InputRate.get(remoteTaskID);
            //double deltaInputQueueLength = (inputRate > procRate) ? (inputRate-procRate)/inputRate : 0;
            double deltaInputQueueLength = 1;
            double inputQueueLength = StatusOfDownStreamTasks.taskID2InQueueLength.get(remoteTaskID) + deltaInputQueueLength;
            StatusOfDownStreamTasks.taskID2InQueueLength.put(remoteTaskID, inputQueueLength);
        }
    }

    public void updateDownStreamTaskStatusOnFailure(int remoteTaskID){
        StatusOfDownStreamTasks.setDownStreamTaskDisconnected(remoteTaskID);
    }

    public void stop(){
        finished = true;
    }
}
