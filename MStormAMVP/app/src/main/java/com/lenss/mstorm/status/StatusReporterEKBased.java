package com.lenss.mstorm.status;

import android.os.SystemClock;

import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.topology.BTask;
import com.lenss.mstorm.utils.Serialization;
import com.lenss.mstorm.utils.StatisticsCalculator;
import com.lenss.mstorm.zookeeper.Assignment;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import edu.tamu.cse.lenss.edgeKeeper.client.EKClient;
import edu.tamu.cse.lenss.edgeKeeper.topology.TopoGraph;

public class StatusReporterEKBased implements Runnable {
    private final String TAG="StatusReporterEKBased";
    Logger logger = Logger.getLogger(TAG);

    private boolean finished = false;

    // Report period to upstream tasks
    public static final int REPORT_PERIOD_TO_UPSTREAM = 10000;  //10s

    Set<Integer> downStreamTasksAll = new HashSet<>();
    Set<String> hostsOfDownStreamTasksAll = new HashSet<>();

    private static class StatusReporterEKBasedHolder {
        public static final StatusReporterEKBased instance = new StatusReporterEKBased();
    }

    public static StatusReporterEKBased getInstance() {
        return StatusReporterEKBased.StatusReporterEKBasedHolder.instance;
    }

    private StatusReporterEKBased(){}

    public void addTaskForMonitoring(int threadID, BTask task) {
        int taskID = task.getTaskID();
        String componentName = task.getComponent();
        StatusOfLocalTasks.task2Component.put(taskID, componentName);
    }

    public void reportAppStatusOfLocalTasksToEdgeKeeper(){
        ReportToUpstreamWithStatusOfLocalTasks reportOfLocalTasks = new ReportToUpstreamWithStatusOfLocalTasks();

        for (Map.Entry<Integer,String> taskEntry: StatusOfLocalTasks.task2Component.entrySet()) {
            int tid = taskEntry.getKey();
            String component = taskEntry.getValue();

            if(StatusOfLocalTasks.task2EmitTimesUpStream.get(tid)!=null && StatusOfLocalTasks.task2EmitTimesUpStream.get(tid).size()!=0){
                // calculate local task status
                double inputRate = StatisticsCalculator.getThroughput(StatusOfLocalTasks.task2EntryTimesUpStream.get(tid), REPORT_PERIOD_TO_UPSTREAM);  // tuple/s
                double outputRate = StatisticsCalculator.getThroughput(StatusOfLocalTasks.task2EmitTimesUpStream.get(tid), REPORT_PERIOD_TO_UPSTREAM);  // tuple/s
                double procRate = 1.0 / StatisticsCalculator.getAvgTime(StatusOfLocalTasks.task2ProcessingTimesUpStream.get(tid)) * 1000.0;   // tuple/s
                double sojournTime = StatisticsCalculator.getAvgTime(StatusOfLocalTasks.task2ResponseTimesUpStream.get(tid));   // ms
                double inQueueLength = MessageQueues.incomingQueues.get(tid).size();
                double outQueueLength = MessageQueues.outgoingQueues.get(tid).size();

                // clear old sample local task status
                StatusOfLocalTasks.task2EntryTimesUpStream.get(tid).clear();
                StatusOfLocalTasks.task2EmitTimesUpStream.get(tid).clear();
                StatusOfLocalTasks.task2ResponseTimesUpStream.get(tid).clear();
                StatusOfLocalTasks.task2ProcessingTimesUpStream.get(tid).clear();

                // update local task status for stream selector
                StatusOfLocalTasks.task2InputRate.put(tid, inputRate);
                StatusOfLocalTasks.task2OutputRate.put(tid, outputRate);
                StatusOfLocalTasks.task2ProcRate.put(tid, procRate);
                StatusOfLocalTasks.task2SojournTime.put(tid, sojournTime);
                StatusOfLocalTasks.task2InQueueLength.put(tid, inQueueLength);
                StatusOfLocalTasks.task2OutQueueLength.put(tid, outQueueLength);

                // update report of local task status
                reportOfLocalTasks.task2InputRate.put(tid, Double.valueOf(String.format("%.2f", inputRate)));
                reportOfLocalTasks.task2OutputRate.put(tid, Double.valueOf(String.format("%.2f", outputRate)));
                reportOfLocalTasks.task2ProcRate.put(tid, Double.valueOf(String.format("%.2f", procRate)));
                reportOfLocalTasks.task2SojournTime.put(tid, Double.valueOf(String.format("%.2f", sojournTime)));
                reportOfLocalTasks.task2InQueueLength.put(tid, Double.valueOf(String.format("%.2f", inQueueLength)));
                reportOfLocalTasks.task2OutQueueLength.put(tid, Double.valueOf(String.format("%.2f", outQueueLength)));

                // record local task status for performance evaluation
                double curTime = SystemClock.elapsedRealtimeNanos()/ 1000000000.0;
                String report = "Time: "           + String.format("%.0f", curTime)        + ","
                              + "TaskID: "         + tid                                   + ","
                              + "Component: "      + component                             + ","
                              + "InputRate: "      + String.format("%.2f", inputRate)      + ","
                              + "OutputRate: "     + String.format("%.2f", outputRate)     + ","
                              + "ProcRate: "       + String.format("%.2f", procRate)       + ","
                              + "SojournTime: "    + String.format("%.2f", sojournTime)    + ","
                              + "InQueueLength: "  + String.format("%.2f", inQueueLength)  + ","
                              + "OutQueueLength: " + String.format("%.2f", outQueueLength) + "\n";
                try {
                    FileWriter fw = new FileWriter(ComputingNode.REPORT_ADDRESSES, true);
                    fw.write(report);
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // debug
                logger.debug(report);
            }
        }

        try {
            // report status of local tasks to EdgeKeeper
            String reportOfLocalTasksStr = Serialization.Serialize(reportOfLocalTasks);
            JSONObject reportOfLocalTasksInJson = new JSONObject(reportOfLocalTasksStr);
            EKClient.putAppStatus("mstorm", reportOfLocalTasksInJson);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public void getDownStreamTasksAndHosts(){
        // get all the downstream tasks of local tasks
        for(Map.Entry<Integer, ConcurrentHashMap<Integer, Integer>> entry: StatusOfLocalTasks.task2taskTupleNum.entrySet()){
            Map<Integer, Integer> downStreamTasks = entry.getValue();
            downStreamTasksAll.addAll(downStreamTasks.keySet());
        }

        // get the hosting devices of all the downstream tasks of local tasks
        Assignment assign = ComputingNode.getAssignment();
        Map<Integer, String> task2Node = assign.getTask2Node();
        for(Integer task: downStreamTasksAll){
            String host = task2Node.get(task);
            hostsOfDownStreamTasksAll.add(host);
        }
    }

    public void getAppStatusOfDownStreamTasksFromEdgeKeeper(){
        // get status of all the downstream tasks of local tasks from hosting devices
        logger.info("====hostsOfDownStreamTasksAll====  "+hostsOfDownStreamTasksAll.toString());
        for(String host: hostsOfDownStreamTasksAll){
            JSONObject appStatus = EKClient.getAppStatus(host, "mstorm");
            logger.info("====appStatusAtHostOfDownStreamTasksAll====    " + host + "," + appStatus);
            if(appStatus!=null) {
                ReportToUpstreamWithStatusOfLocalTasks report = (ReportToUpstreamWithStatusOfLocalTasks) Serialization.Deserialize(appStatus.toString(), ReportToUpstreamWithStatusOfLocalTasks.class);
                StatusOfDownStreamTasks.collectAppStatusOfDownStreamTasks(downStreamTasksAll, report);
            } else {
                logger.error("Fail to get mstorm status from node: " + host);
            }
        }
    }

    public void getNetStatusToDownStreamTasksFromEdgeKeeper(){
        TopoGraph networkInfo = EKClient.getNetworkInfo();
        if(networkInfo!=null){
            StatusOfDownStreamTasks.collectNetStatusOfDownStreamTasks(downStreamTasksAll, networkInfo);
        } else {
            logger.error("Fail to get network status.");
        }
    }

    public void run(){
        finished = false;
        getDownStreamTasksAndHosts();

        while (!Thread.currentThread().isInterrupted() && !finished) {
            try {
                Thread.sleep(REPORT_PERIOD_TO_UPSTREAM);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("The report thread has stopped because of interruption.");
            }
            reportAppStatusOfLocalTasksToEdgeKeeper();
            getAppStatusOfDownStreamTasksFromEdgeKeeper();
            getNetStatusToDownStreamTasksFromEdgeKeeper();
        }

        logger.info("==== Status reporter based on EdgeKeeper stops ====" + finished);
    }

    public void stopReport(){
        finished = true;
    }
}
