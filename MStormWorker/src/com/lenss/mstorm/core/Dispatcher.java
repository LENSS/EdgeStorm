package com.lenss.mstorm.core;

import com.lenss.mstorm.utils.MyPair;
import com.google.gson.Gson;
import com.lenss.mstorm.communication.internodes.ChannelManager;
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
        ArrayList<Integer> localTasks = ComputingNode.getLocalTasks(MStormWorker.localAddress, assignment);
        String serTopology = assignment.getSerTopology();
        Topology topology = new Gson().fromJson(serTopology, Topology.class);
        HashMap<String, Integer> grouping = topology.getGroupings();
        while (!Thread.currentThread().isInterrupted() && !finished) {
            if(localTasks!=null) {
                for (int i = 0; i < localTasks.size(); i++) {
                    int taskID = localTasks.get(i);
                    MyPair<String, byte[]> outdata = ComputingNode.retrieveOutgoingQueue(taskID);
                    if (outdata != null) {
                        //StatusReporter.getInstance().updateIsIncludingTask();
                        if (!outdata.left.equals("END")) {
                            if ((outdata.right != null)) {
                                int remoteTaskID;
                                if (grouping.get(outdata.left) == Topology.Shuffle) {         // Shuffle stream grouping
                                    /** assign computation task to a random server **/
                                    if ((remoteTaskID = ChannelManager.sendToRandomComponentServer(outdata.left, outdata.right)) == -1) { // not sent
                                        try {
                                            ComputingNode.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else {   //sent
                                        long timePoint = System.nanoTime();
                                        ComputingNode.task2EmitTimesUpStream.get(taskID).add(timePoint);
                                        ComputingNode.task2EmitTimesNimbus.get(taskID).add(timePoint);

                                        if(ComputingNode.task2EntryTimes.get(taskID).size()>0) {
                                            long entryTimePoint = ComputingNode.task2EntryTimes.get(taskID).remove(0);
                                            long responseTime = timePoint - entryTimePoint;
                                            ComputingNode.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                                            ComputingNode.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                                        }

                                        if (ComputingNode.task2BeginProcessingTimes.get(taskID).size()>0) {
                                            long beginProcessingTime = ComputingNode.task2BeginProcessingTimes.get(taskID).remove(0);
                                            long processingTime = timePoint - beginProcessingTime;
                                            ComputingNode.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
                                        }

                                        int newTotalTupleNum = ComputingNode.task2taskTupleNum.get(taskID).get(remoteTaskID) + 1;
                                        ComputingNode.task2taskTupleNum.get(taskID).put(remoteTaskID, newTotalTupleNum);
                                        long newTotalTupleSize = ComputingNode.task2taskTupleSize.get(taskID).get(remoteTaskID) + outdata.right.length;
                                        ComputingNode.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
                                    }
                                } else if (grouping.get(outdata.left) == Topology.Feedback_based) {       // Feedback based stream grouping
                                    /** assign computation task to a server calculated based on feedback execution time **/
                                    if ((remoteTaskID = ChannelManager.sendToComponentServerOnFeedBackExeTime(outdata.left, outdata.right)) == -1) { // not sent
                                        try {
                                            ComputingNode.reQueue(taskID, outdata);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    } else { //sent
                                        long timePoint = System.nanoTime();
                                        ComputingNode.task2EmitTimesUpStream.get(taskID).add(timePoint);
                                        ComputingNode.task2EmitTimesNimbus.get(taskID).add(timePoint);

                                        if(ComputingNode.task2EntryTimes.get(taskID).size()>0) {
                                            long entryTimePoint = ComputingNode.task2EntryTimes.get(taskID).remove(0);
                                            long responseTime = timePoint - entryTimePoint;
                                            ComputingNode.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                                            ComputingNode.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                                        }

                                        if (ComputingNode.task2BeginProcessingTimes.get(taskID).size()>0) {
                                            long beginProcessingTime = ComputingNode.task2BeginProcessingTimes.get(taskID).remove(0);
                                            long processingTime = timePoint - beginProcessingTime;
                                            ComputingNode.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
                                        }

                                        int newTotalTupleNum = ComputingNode.task2taskTupleNum.get(taskID).get(remoteTaskID) + 1;
                                        ComputingNode.task2taskTupleNum.get(taskID).put(remoteTaskID, newTotalTupleNum);
                                        long newTotalTupleSize = ComputingNode.task2taskTupleSize.get(taskID).get(remoteTaskID) + outdata.right.length;
                                        ComputingNode.task2taskTupleSize.get(taskID).put(remoteTaskID, newTotalTupleSize);
                                    }
                                }
                            }
                        } else {
                            try {
                                ComputingNode.emitToResultQueue(taskID, outdata);
                                
                                long timePoint = System.nanoTime();
                                ComputingNode.task2EmitTimesUpStream.get(taskID).add(timePoint);
                                ComputingNode.task2EmitTimesNimbus.get(taskID).add(timePoint);


                                if(ComputingNode.task2EntryTimes.get(taskID).size()>0) {
                                    long entryTimePoint = ComputingNode.task2EntryTimes.get(taskID).remove(0);
                                    long responseTime = timePoint - entryTimePoint;
                                    ComputingNode.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                                    ComputingNode.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                                }

                                if (ComputingNode.task2BeginProcessingTimes.get(taskID).size()>0) {
                                    long beginProcessingTime = ComputingNode.task2BeginProcessingTimes.get(taskID).remove(0);
                                    long processingTime = timePoint - beginProcessingTime;
                                    ComputingNode.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
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

