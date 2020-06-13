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

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

public class Dispatcher implements Runnable {
    private final String TAG="Dispatcher";
    Logger logger = Logger.getLogger(TAG);

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
                        //StatusReporter.getInstance().updateIsIncludingTask();
                        if (!outdata.first.equals("END")) { // Go to tasks of the next component
                            if ((outdata.second != null)) {
                                DispatchStatus dispatchStatus;
                                if (grouping.get(outdata.first) == Topology.Shuffle) {         // Shuffle stream grouping
                                    dispatchStatus = ChannelManager.sendToRandomDownstreamTask(outdata.first, outdata.second);
                                    if (!dispatchStatus.success) {
                                        try {
                                            MessageQueues.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
                                    }
                                } else if (grouping.get(outdata.first) == Topology.MinSojournTime) {
                                    dispatchStatus = ChannelManager.sendToDownstreamTaskMinSojournTime(outdata.first, outdata.second, false);
                                    if (!dispatchStatus.success) {
                                        if(dispatchStatus.remoteTaskID!=-1){
                                            updateDownStreamTaskStatusOnFailure(dispatchStatus.remoteTaskID);
                                        }
                                        dispatchStatus = ChannelManager.sendToDownstreamTaskMinSojournTime(outdata.first, outdata.second, true);
                                        if (!dispatchStatus.success) {
                                            if(dispatchStatus.remoteTaskID!=-1){
                                                updateDownStreamTaskStatusOnFailure(dispatchStatus.remoteTaskID);
                                            }
                                            try {
                                                MessageQueues.reQueue(taskID, outdata);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                            updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
                                    }
                                } else if(grouping.get(outdata.first) == Topology.SojournTimeProb){
                                    dispatchStatus = ChannelManager.sendToDownstreamTaskSojournTimeProb(outdata.first, outdata.second, false);
                                    if (!dispatchStatus.success) {
                                        if(dispatchStatus.remoteTaskID!=-1){
                                            updateDownStreamTaskStatusOnFailure(dispatchStatus.remoteTaskID);
                                        }
                                        dispatchStatus = ChannelManager.sendToDownstreamTaskSojournTimeProb(outdata.first, outdata.second, true);
                                        if (!dispatchStatus.success) {
                                            if(dispatchStatus.remoteTaskID!=-1){
                                                updateDownStreamTaskStatusOnFailure(dispatchStatus.remoteTaskID);
                                            }
                                            try {
                                                MessageQueues.reQueue(taskID, outdata);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                            updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
                                    }
                                } else if (grouping.get(outdata.first) == Topology.MinEWT){
                                    dispatchStatus = ChannelManager.sendToDownstreamTaskMinEWT(outdata.first, outdata.second, false);
                                    if (!dispatchStatus.success) {
                                        if(dispatchStatus.remoteTaskID!=-1){
                                            updateDownStreamTaskStatusOnFailure(dispatchStatus.remoteTaskID);
                                        }
                                        dispatchStatus = ChannelManager.sendToDownstreamTaskMinEWT(outdata.first, outdata.second, true);
                                        if (!dispatchStatus.success) {
                                            if(dispatchStatus.remoteTaskID!=-1){
                                                updateDownStreamTaskStatusOnFailure(dispatchStatus.remoteTaskID);
                                            }
                                            try {
                                                MessageQueues.reQueue(taskID, outdata);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                            updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
                                        }
                                    } else {
                                        updateLocalTaskStatus(taskID, dispatchStatus.remoteTaskID, outdata.second.pktSize());
                                        updateDownStreamTaskStatusOnSuccess(dispatchStatus.remoteTaskID);
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

        logger.info("==== Dispatcher stops ====");
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
        StatusOfDownStreamTasks.updateDownStreamTaskLink(remoteTaskID);
    }

    public void stopDispatch(){
        finished = true;
    }
}
