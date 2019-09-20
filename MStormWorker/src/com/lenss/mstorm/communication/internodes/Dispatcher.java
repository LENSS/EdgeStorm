package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.utils.MyPair;
import com.google.gson.Gson;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStormWorker;
import com.lenss.mstorm.status.StatusOfDownStreamTasks;
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
        ArrayList<Integer> localTasks = assignment.getNode2Tasks().get(MStormWorker.GUID);
        
        while (!Thread.currentThread().isInterrupted() && !finished) {
            if(localTasks!=null) {
                for (int taskID: localTasks) {                 
                    MyPair<String, InternodePacket> outdata = MessageQueues.retrieveOutgoingQueue(taskID);
                    if (outdata != null) {
                        //StatusReporter.getInstance().updateIsIncludingTask();
                        if (!outdata.left.equals("END")) {
                            if ((outdata.right != null)) {
                                int remoteTaskID;
                                if (grouping.get(outdata.left) == Topology.Shuffle) {         // Shuffle stream grouping
                                    /** assign computation task to a random server **/
                                    if ((remoteTaskID = ChannelManager.sendToRandomDownstreamTask(outdata.left, outdata.right)) == -1) { // not sent
                                        try {
                                            MessageQueues.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else {   //sent
                                        long timePoint = System.nanoTime();
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
                                        long newTotalTupleSize = StatusOfLocalTasks.task2taskTupleSize.get(taskID).get(remoteTaskID) + outdata.right.toString().length();
                                        StatusOfLocalTasks.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
                                    }
                                } else if (grouping.get(outdata.left) == Topology.Feedback_based) {       // Feedback based stream grouping
                                    /** assign computation task to a server calculated based on feedback execution time **/
                                    if ((remoteTaskID = ChannelManager.sendToDownstreamTaskMinSojournTime(outdata.left, outdata.right)) == -1) { // not sent
                                        try {
                                            MessageQueues.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else { //sent
                                        long timePoint = System.nanoTime();
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
                                        long newTotalTupleSize = StatusOfLocalTasks.task2taskTupleSize.get(taskID).get(remoteTaskID) + outdata.right.toString().length();
                                        StatusOfLocalTasks.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
                                    }
                                }
                            }
                        } else {
                            try {
                                MessageQueues.emitToResultQueue(taskID, outdata);
                                
                                long timePoint = System.nanoTime();
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
            }
            else{
                finished = true;
            }
        }
        System.out.println("The Dispatcher thread has stopped ... ");
    }

    public void stop(){
        finished = true;
    }
}

