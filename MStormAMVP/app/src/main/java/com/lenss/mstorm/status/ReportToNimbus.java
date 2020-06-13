package com.lenss.mstorm.status;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.core.MStorm;

import java.util.HashMap;
import java.util.Map;

public class ReportToNimbus {
    @Expose
    public double availability;

    //// STATUS ABOUT CPU AND WORKLOAD IN MHZ
    // availCPUForMStormTasks = cpuFrequency * cpuCoreNum - (cpuUsage - taskID2CPUUsage)
    @Expose
    public double cpuFrequency;
    @Expose
    public int cpuCoreNum;
    @Expose
    public double cpuUsage;
    @Expose
    public double availCPUForMStormTasks;
    @Expose
    public Map<Integer, Double> taskID2CPUUsage;

    // Check the CPU usage of mstorm platform threads, not sure whether we need it
    //@Expose
    //public Map<String, Double> pfThreadName2CPUUsage;

    //// STATUS ABOUT MEMORY (For future use)
    @Expose
    public double availableMemory;   // MB

    //// STATUS ABOUT NETWORK
    @Expose
    public double txBandwidth;
    @Expose
    public double rxBandwidth;
    @Expose
    public Map<String, Double> rttMap; // RTT to other devices
    @Expose
    public Map<String, Double> linkQualityMap; // linkQuality to other devices
    // For future use
    @Expose
    public double wifiLinkSpeed;
    @Expose
    public double rSSI;

    //// STATUS ABOUT BATTERY
    @Expose
    public double batteryCapacity;
    @Expose
    public double batteryLevel;

    //// PARAMETERS ABOUT ENERGY CONSUMPTION PER BIT FOR WIFI TX/RX
    @Expose
    public double energyPerBitTx;       //nJ/bit
    @Expose
    public  double energyPerBitRx;   //nJ/bit

    //// STATUS ABOUT TRAFFICS FROM TASK TO TASK
    @Expose
    public boolean isIncludingTaskReport;
    @Expose
    public Map<Integer, Map<Integer, Double>> task2TaskTupleRate;
    @Expose
    public Map<Integer, Map<Integer, Double>> task2TaskTupleAvgSize;

    //// STATUS ABOUT TUPLE INPUT/OUTPUT RATE AND DELAY
    @Expose
    public Map<Integer, Double> task2Output;
    @Expose
    public Map<Integer, Double> task2Delay;
    @Expose
    public Map<Integer, Double> task2Input;
    @Expose
    public Map<Integer, Double> task2StreamInput;

    public ReportToNimbus(){
        isIncludingTaskReport = false;
        availability = MStorm.availability;
        taskID2CPUUsage = new HashMap<Integer,Double>();
        // pfThreadName2CPUUsage = new HashMap<String, Double>();
        rttMap = new HashMap<String, Double>();
        linkQualityMap = new HashMap<String, Double>();
        task2TaskTupleRate = new HashMap<Integer, Map<Integer, Double>>();
        task2TaskTupleAvgSize = new HashMap<Integer, Map<Integer, Double>>();
        task2Output = new HashMap<Integer,Double>();
        task2Delay = new HashMap<Integer,Double>();
        task2Input = new HashMap<Integer, Double>();
        task2StreamInput = new HashMap<Integer, Double>();
    }
}
