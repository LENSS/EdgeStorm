package com.lenss.mstorm.status;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import android.app.ActivityManager;
import android.app.ActivityManager.MemoryInfo;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.TrafficStats;
import android.net.wifi.WifiInfo;
import android.net.wifi.WifiManager;
import android.os.*;
import android.os.Process;

import com.lenss.mstorm.communication.internodes.ChannelManager;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.executor.Executor;
import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.communication.masternode.MasterNodeClient;
import com.lenss.mstorm.communication.masternode.Request;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.status.bandwidth.ConnectionClassManager;
import com.lenss.mstorm.status.bandwidth.DeviceBandwidthSampler;
import com.lenss.mstorm.status.d2drtt.D2DRTTSampler;
import com.lenss.mstorm.utils.Serialization;
import com.lenss.mstorm.utils.StatisticsCalculator;

import org.apache.log4j.Logger;


/**
 * Created by cmy on 8/3/16.
 */
public class StatusReporter implements Runnable {
    /// LOGGER
    private final String TAG="StatusReporter";
    Logger logger = Logger.getLogger(TAG);

    //// PARAMETERS FOR DEBUGGING AND REPORTING
    private Handler mHandler;
    private Context context;
    private MasterNodeClient masterNodeClient;
    private String cluster_id;
    private ReportToNimbus report2Nimbus;
    //private boolean finished;

    //// AUXILIARY PARAMETERS FOR CALCULATING CPU WORKLOAD STATUS
    // Task ID to component
    private Map<Integer,String> task2Component;
    // Task ID to thread ID
    private Map<Integer,Integer> task2Thread;
    // Thread ID to task ID
    private Map<Integer, Integer> thread2Task;

    /*MStorm platform threads, such as Dispatcher, StatusReporter...*/
    // private static Map<Integer, String> threadID2PFThreadName= new HashMap<Integer, String>();

    // Map<threadID, last CPU time (jiffies)>
    private Map<Integer, Long> threadID2LastCPUTime;
    // Current CPU time (jiffies) in user, system or nice modes
    private long curBusyCPUTime = 0;
    // Current CPU time (jiffies) in all modes
    private long curTotalCPUTime = 0;
    // Total CPU usage (jiffies) in this sampling period
    private long sampledTotalCPUUsage = 0;

    //// AUXILIARY PARAMETERS FOR CALCULATING NETWORK STATUS
    //private ConnectionClassManager mConnectionClassManager;
    //private DeviceBandwidthSampler mDeviceBandwidthSampler;
    private D2DRTTSampler mD2DRTTSampler;
    private static Map<String, Double> rttMap= new ConcurrentHashMap<String, Double>();

    private long lastTimePointNimbus;
    private long lastTxBytes;
    private long lastRxBytes;
    private static final int BYTES_TO_BITS = 8;
    /* NOTE: Eb = alpha/throughPut + beta (nJ/bit), Following model is universal model*/
    private static final double TX_ENERGY_THROUGHPUT_ALPHA = 229.4;
    private static final double TX_ENERGY_THROUGHPUT_BETA  = 23.5;
    private static final double RX_ENERGY_THROUGHPUT_ALPHA = 229.4;
    private static final double RX_ENERGY_THROUGHPUT_BETA  = 23.5;

    //// AUXILIARY PARAMETERS FOR REPORTING
    public static final double END2ENDDELAY_THRESHOLD = 2000.0;
    // Report to Nimbus every 30s
    public static final int REPORT_PERIOD_TO_NIMBUS = 15000;
    // Report to upstream tasks every 5s
    public static final int REPORT_PERIOD_TO_UPSTREAM = 5000;
    public static final int THROUGHPUT_NIMBUS = 1;
    public static final int THROUGHPUT_UPSTREAM = 2;
    // Report period ratio 6
    public static final int PERIOD_RATIO = REPORT_PERIOD_TO_NIMBUS/REPORT_PERIOD_TO_UPSTREAM;
    private static int periodCounter = 0;


    private static class StatusReporterHolder {
        public static final StatusReporter instance = new StatusReporter();
    }

    private StatusReporter(){}

    public static StatusReporter getInstance() {
        return StatusReporterHolder.instance;
    }

    public void initializeStatusReporter(Handler mHandler, Context context, MasterNodeClient masterNodeClient, String cluster_id) {
        this.mHandler = mHandler;
        this.context = context;
        this.masterNodeClient = masterNodeClient;
        this.cluster_id = cluster_id;
        //finished = false;
        report2Nimbus = new ReportToNimbus();
        task2Component = new HashMap<Integer,String>();
        task2Thread = new HashMap<Integer,Integer>();
        thread2Task = new HashMap<Integer, Integer>();
        threadID2LastCPUTime= new HashMap<Integer, Long>();
    }


    //// METHODS FOR CPU STATUS
    // Get the cpu frequency in MHz
    private void updateCPUMaxFrequency(){
        double cpuMaxFreq = 0;
        try {
            RandomAccessFile reader = new RandomAccessFile("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq", "r");
            String MaxFreq = reader.readLine();
            cpuMaxFreq = Double.parseDouble(MaxFreq);      // the unit is KHz
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        report2Nimbus.cpuFrequency = cpuMaxFreq / 1000.0;
    }

    // Get the cpu core number
    private void updateCPUCoreNum() {
        class CPUFilter implements FileFilter {
            @Override
            public boolean accept(File pathname) {
                // Check if filename is "cpu0", "cpu1,...
                if (Pattern.matches("cpu[0-9]", pathname.getName())) {
                    return true;
                }
                return false;
            }
        }
        try {
            // Get directory containing CPU info
            File dir = new File("/sys/devices/system/cpu/");
            File[] files = dir.listFiles(new CPUFilter());
            // Return the number of cores
            report2Nimbus.cpuCoreNum = files.length;
        } catch (Exception e) {
            e.printStackTrace();
            report2Nimbus.cpuCoreNum = 1;
        }
    }

    private void updateWorkingCPUCoreNum() {
        int workingCPU = 0;
        try {
            RandomAccessFile reader = new RandomAccessFile("/sys/devices/system/cpu/online", "r");
            String onlineCPUs = reader.readLine();
            String[] onlineCPUArray = onlineCPUs.split(",");

            for(String str: onlineCPUArray){
                if (str.contains("-")){
                    String[] CPUs = str.split("-");
                    int maxCPUID = Integer.parseInt(CPUs[1]);
                    int minCPUID = Integer.parseInt(CPUs[0]);
                    workingCPU += maxCPUID-minCPUID+1;
                }
                else
                    workingCPU ++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        report2Nimbus.cpuCoreNum = workingCPU;
    }


    // Update the total and busy CPU time from file /proc/stat
    public void updateCPUTimeFromFile(){
        System.out.println("updateCPUTimeFromFile:" + SystemClock.elapsedRealtimeNanos());
        try {
            RandomAccessFile reader = new RandomAccessFile("/proc/stat", "r");
            String cpuTime = reader.readLine();
            String[] cpuTimeTokens = cpuTime.split(" +");  // Split on one or more spaces
            curTotalCPUTime = Long.parseLong(cpuTimeTokens[1]) + Long.parseLong(cpuTimeTokens[2]) + Long.parseLong(cpuTimeTokens[3]) +
                    Long.parseLong(cpuTimeTokens[4]) + Long.parseLong(cpuTimeTokens[5]) + Long.parseLong(cpuTimeTokens[6]) + Long.parseLong(cpuTimeTokens[7]) + Long.parseLong(cpuTimeTokens[8]);
            curBusyCPUTime = curTotalCPUTime - Long.parseLong(cpuTimeTokens[4]) - Long.parseLong(cpuTimeTokens[5]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Update the thread CPU time from file /proc/pid/tasks/tid/stat
    public void updateThreadCPUTimeFromFile(int processID, int threadID){
        System.out.println("updateThreadCPUTimeFromFile:"+SystemClock.elapsedRealtimeNanos());
        long curThreadCPUTime = 0;
        try {
            RandomAccessFile reader = new RandomAccessFile("/proc/" + processID + "/task/" + threadID + "/stat", "r");
            String threadUsageInfo = reader.readLine();
            String[] threadUsageTokens = threadUsageInfo.split(" +");  // Split on one or more spaces
            curThreadCPUTime = Long.parseLong(threadUsageTokens[13]) + Long.parseLong(threadUsageTokens[14]);
        } catch (IOException e ){
            e.printStackTrace();
        }
        System.out.println("thread:"+threadID+",\t"+"curThreadCPUTime:"+curThreadCPUTime);
        threadID2LastCPUTime.put(threadID, curThreadCPUTime);
    }


    // Add MStorm Executors (Tasks) for monitoring
    public void addTaskForMonitoring(int threadID, Integer taskID, String componentName){
        System.out.println("add thread:"+threadID);
        task2Component.put(taskID,componentName);
        thread2Task.put(threadID, taskID);
        task2Thread.put(taskID,threadID);
        //threadID2LastCPUTime.put(threadID, new Long(0));
        HashMap<Integer, Double> task2TupleRate = new HashMap<Integer, Double>();
        report2Nimbus.task2TaskTupleRate.put(taskID, task2TupleRate);
        HashMap<Integer, Double> task2TupleAvgSize = new HashMap<Integer, Double>();
        report2Nimbus.task2TaskTupleAvgSize.put(taskID, task2TupleAvgSize);
    }

    public static void removeTaskForMonitoring(){
                //TODO: remove tasks
    }

    // Add MStorm Platform Threads for monitoring
/*    public static void addPFThreadForMonitoring(int threadID, String threadName){
        threadID2PFThreadName.put(threadID, threadName);
        threadID2LastCPUTime.put(threadID, new Long(0));
    }*/

    // Get Total CPU usage in MHz
    private void updateTotalCPUUsage() {
        long oldTotalCPUTime = curTotalCPUTime;
        long oldBusyCPUTime = curBusyCPUTime;
        updateCPUTimeFromFile();   // update CPU time
        long busyCPUUsage = curBusyCPUTime-oldBusyCPUTime;
        sampledTotalCPUUsage = curTotalCPUTime-oldTotalCPUTime;
        double cpuFrequency = report2Nimbus.cpuFrequency;
        int cpuCoreNum = report2Nimbus.cpuCoreNum;
        if(sampledTotalCPUUsage!=0.0)
            report2Nimbus.cpuUsage = Math.round(1.0*busyCPUUsage/sampledTotalCPUUsage*cpuFrequency*cpuCoreNum * 10) / 10.0;
        else
            report2Nimbus.cpuUsage = -1.0;
    }

    // Update the CPU usage of all tasks
    public void updateCPUUsageOfTasks(){
        int processID = ComputingNode.getProcessID();   // Get the processID for executor threads

        double totalUsageOfmStormTasks = 0.0;
        double cpuFrequency = report2Nimbus.cpuFrequency;
        int cpuCoreNum = report2Nimbus.cpuCoreNum;
        for (Map.Entry<Integer,Integer> thread2TaskEntry: thread2Task.entrySet()) {
            int threadID = thread2TaskEntry.getKey();
            long lastThreadCPUTime = threadID2LastCPUTime.get(threadID);
            updateThreadCPUTimeFromFile(processID, threadID);
            long currentThreadCPUTime = threadID2LastCPUTime.get(threadID);
            long threadCPUUsage = currentThreadCPUTime-lastThreadCPUTime;
            double threadUsage;
            if(sampledTotalCPUUsage!=0.0)
                threadUsage = 1.0*threadCPUUsage/sampledTotalCPUUsage*cpuFrequency*cpuCoreNum;
            else
                threadUsage = -1.0;
            int taskID = thread2TaskEntry.getValue();
            report2Nimbus.taskID2CPUUsage.put(taskID,threadUsage);
            totalUsageOfmStormTasks+=threadUsage;
        }
        report2Nimbus.availCPUForMStormTasks = Math.round((cpuFrequency * cpuCoreNum - report2Nimbus.cpuUsage  + totalUsageOfmStormTasks) * 10) / 10.0;
    }

    // update the CPU usage of MStorm platform threads
/*    public void updateCPUUsageOfPFThreads(){
        int processID = Supervisor.getProcessID();   // Get the processID for mstorm platform threads
        Iterator<Map.Entry<Integer,String>> it = threadID2PFThreadName.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, String> threadID2PFThreadNameEntry = it.next();
            int threadID = threadID2PFThreadNameEntry.getKey();
            long previousThreadCPUTime = threadID2LastCPUTime.get(threadID);
            updateThreadCPUTimeFromFile(processID, threadID);
            long currentThreadCPUTime = threadID2LastCPUTime.get(threadID);
            long threadCPUUsage = currentThreadCPUTime-previousThreadCPUTime;
            double cpuFrequency = report2Nimbus.cpuFrequency;
            int cpuCoreNum = report2Nimbus.cpuCoreNum;
            double threadUsage = 1.0*threadCPUUsage/sampledTotalCPUUsage*cpuFrequency*cpuCoreNum;
            String threadName = threadID2PFThreadNameEntry.getValue();
            report2Nimbus.pfThreadName2CPUUsage.put(threadName,threadUsage);
        }
    }*/


    //// METHODS FOR MEMORY STATUS
    // update the available memories
    public void updateAvailableMemory(Context context) {
        MemoryInfo mi = new MemoryInfo();
        ActivityManager activityManager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
        activityManager.getMemoryInfo(mi);
        report2Nimbus.availableMemory = mi.availMem / 1048576L;  // MB
    }


    //// METHODS FOR NETWORK
    // update the RX Bandwidth
    /*public void updateRxBandwidth(){
        report2Nimbus.rxBandwidth = Math.round(mConnectionClassManager.getDownloadKBitsPerSecond() * 10) / 10.0;
    }*/

    // update the TX Bandwidth
    /*public void updateTxBandwidth(){
        report2Nimbus.txBandwidth = Math.round(mConnectionClassManager.getUploadKBitsPerSecond() * 10) / 10.0;
    }*/

    // add RTT time in ms to devices
    public static void addRTT2Device(String ipAddress, double rtt){
        rttMap.put(ipAddress,rtt);
    }

    // update RTT Time in ms to devices
    public void updateRTT2Device(){
        for(Map.Entry<String, Double> rttEntry: rttMap.entrySet()){
            report2Nimbus.rttMap.put(rttEntry.getKey(),rttEntry.getValue());
        }
    }

    // update the link speed
    public void updateWifiLinkSpeed(Context context) {
        WifiManager myWifiManager = (WifiManager) context.getSystemService(Context.WIFI_SERVICE);
        WifiInfo myWifiInfo = myWifiManager.getConnectionInfo();
        report2Nimbus.wifiLinkSpeed = myWifiInfo.getLinkSpeed();
    }

    // update the RSSI
    public void updateRSSI(Context context) {
        WifiManager myWifiManager = (WifiManager) context.getSystemService(Context.WIFI_SERVICE);
        WifiInfo myWifiInfo = myWifiManager.getConnectionInfo();
        report2Nimbus.rSSI = myWifiInfo.getRssi();
    }


    //// METHODS FOR BATTERY POWER
    // update the battery capacity
    public void updateBatteryCapacity(Context context) {
        Object mPowerProfile_ = null;
        final String POWER_PROFILE_CLASS = "com.android.internal.os.PowerProfile";
        try {
            mPowerProfile_ = Class.forName(POWER_PROFILE_CLASS)
                    .getConstructor(Context.class).newInstance(context);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            report2Nimbus.batteryCapacity = (double) Class.forName(POWER_PROFILE_CLASS)
                    .getMethod("getAveragePower", java.lang.String.class)
                    .invoke(mPowerProfile_, "battery.capacity");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // update the battery level
    public void updateBatteryLevel(Context context) {
        Intent batteryIntent = context.registerReceiver(null, new IntentFilter(Intent.ACTION_BATTERY_CHANGED));
        int level = batteryIntent.getIntExtra(BatteryManager.EXTRA_LEVEL, -1);
        int scale = batteryIntent.getIntExtra(BatteryManager.EXTRA_SCALE, -1);
        if(scale !=0)
            report2Nimbus.batteryLevel = ((double) level / (double) scale) * 100.0f;   // 0~100
        else
            report2Nimbus.batteryLevel = -1.0;
    }

    //// METHODS FOR ENERGY PER BIT
    public void updateEnergyPerBitTx(){
        long curTime = SystemClock.elapsedRealtimeNanos();
        double TimeDiff = (curTime-lastTimePointNimbus);  // ns
        long curTxBytes = TrafficStats.getTotalTxBytes();
        long TxDiff = (curTxBytes - lastTxBytes);
        lastTxBytes = curTxBytes;
        double txThroughPut = 1.0*TxDiff/TimeDiff*BYTES_TO_BITS*1000.0;  // Mbps
        report2Nimbus.txBandwidth = txThroughPut;
        if(txThroughPut!=0.0)
            report2Nimbus.energyPerBitTx = Math.round((TX_ENERGY_THROUGHPUT_ALPHA/txThroughPut+TX_ENERGY_THROUGHPUT_BETA) * 10) / 10.0;
        else
            report2Nimbus.energyPerBitTx = -1.0;
    }

    public void updateEnergyPerBitRx(){
        long curTime = SystemClock.elapsedRealtimeNanos();
        double TimeDiff = (curTime-lastTimePointNimbus);  // ns
        long curRxBytes = TrafficStats.getTotalRxBytes();
        long RxDiff = (curRxBytes - lastRxBytes);
        lastRxBytes = curRxBytes;
        double rxThroughPut = 1.0*RxDiff/TimeDiff*BYTES_TO_BITS*1000.0; // Mbps
        report2Nimbus.rxBandwidth = rxThroughPut;
        if(rxThroughPut!=0.0)
            report2Nimbus.energyPerBitRx = Math.round((RX_ENERGY_THROUGHPUT_ALPHA/rxThroughPut+RX_ENERGY_THROUGHPUT_BETA) * 10) / 10.0;
        else
            report2Nimbus.energyPerBitRx = -1.0;
    }

    //// METHODS FOR TRAFFICS
    // update tuple throughput and delay (For upstream)
    private void updateTaskTrafficReportToUpstream(){
        for (Map.Entry<Integer,String> taskEntry: task2Component.entrySet()) {
            int tid = taskEntry.getKey();
            String component = taskEntry.getValue();

            if(StatusOfLocalTasks.task2EmitTimesUpStream.get(tid)!=null && StatusOfLocalTasks.task2EmitTimesUpStream.get(tid).size()!=0){
                double inputRate = StatisticsCalculator.getThroughput(StatusOfLocalTasks.task2EntryTimes.get(tid),THROUGHPUT_UPSTREAM);          // tuple/s
                double outputRate = StatisticsCalculator.getThroughput(StatusOfLocalTasks.task2EmitTimesUpStream.get(tid),THROUGHPUT_UPSTREAM);  // tuple/s
                double processingTime = StatisticsCalculator.getAvgTime(StatusOfLocalTasks.task2ProcessingTimesUpStream.get(tid));            // ms
                double procRate = 1/processingTime;
                double sojournTime = StatisticsCalculator.getAvgTime(StatusOfLocalTasks.task2ResponseTimesUpStream.get(tid));                 // ms

                StatusOfLocalTasks.task2EmitTimesUpStream.get(tid).clear();
                StatusOfLocalTasks.task2ResponseTimesUpStream.get(tid).clear();
                StatusOfLocalTasks.task2ProcessingTimesUpStream.get(tid).clear();

                double inQueueLength = MessageQueues.incomingQueues.get(tid).size();
                double outQueueLength = MessageQueues.outgoingQueues.get(tid).size();

                InternodePacket pkt = new InternodePacket();
                pkt.type = InternodePacket.TYPE_REPORT;
                pkt.fromTask = tid;
                pkt.toTask = -1; // broadcast
                pkt.simpleContent.put("procRate", String.format("%.2f", procRate));
                pkt.simpleContent.put("inputRate", String.format("%.2f", inputRate));
                pkt.simpleContent.put("outputRate", String.format("%.2f", outputRate));
                pkt.simpleContent.put("inQueueLength", String.format("%.2f", inQueueLength));
                pkt.simpleContent.put("outQueueLength", String.format("%.2f", outQueueLength));
                pkt.simpleContent.put("sojournTime", String.format("%.2f", sojournTime));
                ChannelManager.broadcastToUpstreamTasks(tid, pkt);

                // record execution information for file
                long curTime = SystemClock.elapsedRealtimeNanos();
                String report = String.format("%.0f", curTime / 1000000000.0) + "," + tid + "," + component + ","
                              + String.format("%.2f", procRate) + ","
                              + String.format("%.2f", inputRate) + ","
                              + String.format("%.2f", outputRate) + ","
                              + String.format("%.2f", inQueueLength) + ","
                              + String.format("%.2f", outQueueLength) + ","
                              + String.format("%.2f", sojournTime) + "\n";
                try {
                    FileWriter fw = new FileWriter(ComputingNode.EXEREC_ADDRESSES, true);
                    fw.write(report);
                    fw.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // update the traffics from task to task (For Nimbus)
    private void updateTaskTrafficReportToNimbus(){

        long curTimePoint = SystemClock.elapsedRealtimeNanos();
        double timeDiff = (curTimePoint - lastTimePointNimbus)/1000000000.0;

        System.out.println("DiffInTraffic:"+timeDiff);

        for (Map.Entry<Integer,String> taskEntry: task2Component.entrySet()) {
            int tid = taskEntry.getKey();

            // Get the input speed of app
            if(StatusOfLocalTasks.task2EntryTimesForFstComp.get(tid)!=null &&
                    StatusOfLocalTasks.task2EntryTimesForFstComp.get(tid).size()!=0){ // this is a task of the first component
                double input = StatisticsCalculator.getInput(StatusOfLocalTasks.task2EntryTimesForFstComp.get(tid));  // tuple/s
                //double input = ComputingNode.task2EntryTimesForFstComp.get(tid).size() / timeDiff;
                report2Nimbus.task2StreamInput.put(tid,input);
                StatusOfLocalTasks.task2EntryTimesForFstComp.get(tid).clear();
            }

            // Get the status for task
            if(StatusOfLocalTasks.task2EmitTimesNimbus.get(tid)!=null &&
                    StatusOfLocalTasks.task2EmitTimesNimbus.get(tid).size()!=0) {
                //double throughput = StatisticsCalculator.getThroughput(ComputingNode.task2EmitTimesNimbus.get(tid),THROUGHPUT_NIMBUS); // tuple/s
                double throughput = StatusOfLocalTasks.task2EmitTimesNimbus.get(tid).size() * 1.0 / timeDiff;
                double avgResponseTime = StatisticsCalculator.getAvgTime(StatusOfLocalTasks.task2ResponseTimesNimbus.get(tid));  // ms
                report2Nimbus.task2Throughput.put(tid, throughput);
                report2Nimbus.task2Delay.put(tid, avgResponseTime);
                StatusOfLocalTasks.task2EmitTimesNimbus.get(tid).clear();        // start from the beginning
                StatusOfLocalTasks.task2ResponseTimesNimbus.get(tid).clear();    // start from the beginning

                // Average tuple rate from tid to any downStream task
                for (ConcurrentHashMap.Entry<Integer, Integer> task2TupleNumEntry : StatusOfLocalTasks.task2taskTupleNum.get(tid).entrySet()) {
                    int remoteTaskID = task2TupleNumEntry.getKey();
                    double tupleRate = 1.0 * task2TupleNumEntry.getValue() / timeDiff;
                    report2Nimbus.task2TaskTupleRate.get(tid).put(remoteTaskID, tupleRate);
                }

                // Average tuple size from tid to any downStream task
                for (ConcurrentHashMap.Entry<Integer, Long> task2TupleSizeEntry : StatusOfLocalTasks.task2taskTupleSize.get(tid).entrySet()) {
                    int remoteTaskID = task2TupleSizeEntry.getKey();
                    long totalTupleSize = task2TupleSizeEntry.getValue();
                    int tupleNum = StatusOfLocalTasks.task2taskTupleNum.get(tid).get(remoteTaskID);
                    double avgTupleSize;
                    if(tupleNum != 0)
                        avgTupleSize = 1.0 * totalTupleSize / tupleNum;
                    else
                        avgTupleSize = 0.0;
                    report2Nimbus.task2TaskTupleAvgSize.get(tid).put(remoteTaskID, avgTupleSize);

                    task2TupleSizeEntry.setValue(new Long(0));          // start from beginning
                    StatusOfLocalTasks.task2taskTupleNum.get(tid).put(remoteTaskID,0);
                }
            }
        }
    }

    //// METHODS FOR UPDATING AND REPORT
    // update all phone status
    public void updatePhoneStatus() {

        updateCPUMaxFrequency();
        updateWorkingCPUCoreNum();

        //// update CPU workload
        updateTotalCPUUsage();
        updateCPUUsageOfTasks();
        //updateCPUUsageOfPFThreads();

        //// update memory
        updateAvailableMemory(context);

        //// update network
        //updateTxBandwidth();
        //updateRxBandwidth();
        //mConnectionClassManager.reset();
        updateRTT2Device();
        updateWifiLinkSpeed(context);
        updateRSSI(context);

        //// update battery
        updateBatteryCapacity(context);
        updateBatteryLevel(context);

        //// update energy per bit
        updateEnergyPerBitTx();
        updateEnergyPerBitRx();
    }

    public String getPhoneStatusForDebug() {
        String phoneStatus = "CU(MHz):"+report2Nimbus.cpuUsage + ",\n" +
                             "ACM(MHz):"+report2Nimbus.availCPUForMStormTasks + ",\n" +
                             "RX:"+report2Nimbus.rxBandwidth + ",\n" +
                             "TX:"+report2Nimbus.txBandwidth + ",\n" +
                             "EBR:"+report2Nimbus.energyPerBitRx + ",\n" +
                             "EBT:"+report2Nimbus.energyPerBitTx;
        return phoneStatus;
    }

    public String getPhoneStatusToNimbus(){
        String phoneStatusToNimbus = Serialization.Serialize(report2Nimbus);
        return phoneStatusToNimbus;
    }

    public void submitPhoneStatusToNimbus(String phoneStatus) {
        Request req = new Request();
        req.setReqType(Request.PHONESTATUS);
        req.setClusterID(cluster_id);
        req.setContent(phoneStatus);
        req.setGUID(MStorm.GUID);
        masterNodeClient.sendRequest(req);
    }

    public void stopSampling(){
/*        if(mDeviceBandwidthSampler!=null) {
            if (mDeviceBandwidthSampler.isSampling()) {
                mDeviceBandwidthSampler.stopSampling();
            }
        }*/
        if(mD2DRTTSampler!=null){
            if(mD2DRTTSampler.isSampling()){
                mD2DRTTSampler.stopSampling();
            }
        }
    }

    public void run() {
        // Start sampling tx/rx bandwidth
        //mConnectionClassManager = ConnectionClassManager.getInstance();
        //mDeviceBandwidthSampler = DeviceBandwidthSampler.getInstance();
        //mDeviceBandwidthSampler.setUID(Process.myUid());
        //mDeviceBandwidthSampler.startSampling();

        // Set some initial values and unchanging values

        lastTimePointNimbus = SystemClock.elapsedRealtimeNanos();

        lastTxBytes = TrafficStats.getTotalTxBytes();
        lastRxBytes = TrafficStats.getTotalRxBytes();

        // Start sampling D2D rtt time
        mD2DRTTSampler = D2DRTTSampler.getInstance();
        ArrayList<String> ipAddresses = Supervisor.newAssignment.getIpAddresses();
        mD2DRTTSampler.setIpAddresses(ipAddresses);
        mD2DRTTSampler.startSampling();

        updateCPUMaxFrequency();
        updateWorkingCPUCoreNum();

        updateCPUTimeFromFile();
        int processID = ComputingNode.getProcessID();
        for (Map.Entry<Integer,Integer> thread2TaskEntry: thread2Task.entrySet()) {
            int threadID = thread2TaskEntry.getKey();
            updateThreadCPUTimeFromFile(processID, threadID);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(REPORT_PERIOD_TO_UPSTREAM);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("The report thread has stopped because of [interruption] ...");
                break;
            }
            //updateTaskTrafficReportToUpstream();
            periodCounter++;

            if(periodCounter==PERIOD_RATIO) {
                updateTaskTrafficReportToNimbus();
                updatePhoneStatus();
                lastTimePointNimbus = SystemClock.elapsedRealtimeNanos();
                String phoneStatusToNimbus = getPhoneStatusToNimbus();
                logger.info("PERIODICAL REPORT TO NIMBUS: " + phoneStatusToNimbus);

                if(masterNodeClient.getChannel()!=null) {
                    submitPhoneStatusToNimbus(phoneStatusToNimbus);
                    Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, "[" + sdf.format(new Date()) + "]" + " A Status Report Sent to Master").sendToTarget();
                }

                // Report phone status to device screen for debugging
                String phoneStatusDebug = getPhoneStatusForDebug();
                mHandler.obtainMessage(MStorm.Message_PERIOD_REPORT, phoneStatusDebug).sendToTarget();

                periodCounter = 0;
            }
        }

        logger.debug("The reporter thread has been stopped ... ");
    }

    public void updateIsIncludingTask(){
        report2Nimbus.isIncludingTaskReport = true;
    }
}
