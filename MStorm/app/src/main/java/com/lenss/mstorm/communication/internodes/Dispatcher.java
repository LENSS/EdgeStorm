package com.lenss.mstorm.communication.internodes;

import android.os.SystemClock;
import android.util.Pair;

import com.google.gson.Gson;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.zookeeper.Assignment;
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
                                int remoteTaskID;
                                if (grouping.get(outdata.first) == Topology.Shuffle) {         // Shuffle stream grouping
                                    if ((remoteTaskID = ChannelManager.sendToRandomDownstreamTask(outdata.first, outdata.second)) == -1) {
                                        try {
                                            MessageQueues.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else { // Todo, make sure the way of recording these statuses is correct
                                        long timePoint = SystemClock.elapsedRealtimeNanos();
                                        StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(timePoint);
                                        StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(timePoint);

                                        if(StatusOfLocalTasks.task2EntryTimes.get(taskID).size()>0) {
                                            long entryTimePoint = StatusOfLocalTasks.task2EntryTimes.get(taskID).remove(0);
                                            long responseTime = timePoint - entryTimePoint;
                                            StatusOfLocalTasks.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                                            StatusOfLocalTasks.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                                        }

                                        if (StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).size()>0) {
                                            long beginProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).remove(0);
                                            long processingTime = timePoint - beginProcessingTime;
                                            StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
                                        }

                                        int newTotalTupleNum = StatusOfLocalTasks.task2taskTupleNum.get(taskID).get(remoteTaskID) + 1;
                                        StatusOfLocalTasks.task2taskTupleNum.get(taskID).put(remoteTaskID, newTotalTupleNum);

                                        long newTotalTupleSize = StatusOfLocalTasks.task2taskTupleSize.get(taskID).get(remoteTaskID) + outdata.second.toString().length();
                                        StatusOfLocalTasks.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
                                    }
                                } else if (grouping.get(outdata.first) == Topology.Feedback_based) {       // Feedback based stream grouping
                                    if ((remoteTaskID = ChannelManager.sendToDownstreamTaskMinSojournTime(outdata.first, outdata.second)) == -1) { // not sent
                                        try {
                                            MessageQueues.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else { // Todo, make sure the way of recording these statuses is correct
                                        long timePoint = SystemClock.elapsedRealtimeNanos();
                                        StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(timePoint);
                                        StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(timePoint);

                                        if(StatusOfLocalTasks.task2EntryTimes.get(taskID).size()>0) {
                                            long entryTimePoint = StatusOfLocalTasks.task2EntryTimes.get(taskID).remove(0);
                                            long responseTime = timePoint - entryTimePoint;
                                            StatusOfLocalTasks.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                                            StatusOfLocalTasks.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                                        }

                                        if (StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).size()>0) {
                                            long beginProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).remove(0);
                                            long processingTime = timePoint - beginProcessingTime;
                                            StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
                                        }

                                        int newTotalTupleNum = StatusOfLocalTasks.task2taskTupleNum.get(taskID).get(remoteTaskID) + 1;
                                        StatusOfLocalTasks.task2taskTupleNum.get(taskID).put(remoteTaskID, newTotalTupleNum);
                                        long newTotalTupleSize = StatusOfLocalTasks.task2taskTupleSize.get(taskID).get(remoteTaskID) + outdata.second.toString().length();
                                        StatusOfLocalTasks.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
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

                                if (StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).size()>0) {
                                    long beginProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).remove(0);
                                    long processingTime = timePoint - beginProcessingTime;
                                    StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
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

    public void stop(){
        finished = true;
    }
}
