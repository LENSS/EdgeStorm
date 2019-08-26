package status;

import com.google.gson.annotations.Expose;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cmy on 1/15/17.
 */
public class ReportToNimbus {
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
    // Check the CPU usage of mstorm platform threads
    @Expose
    public Map<String, Double> pfThreadName2CPUUsage;

    //// STATUS ABOUT MEMORY (For future use)
    @Expose
    public double availableMemory;   // MB

    //// STATUS ABOUT NETWORK
    @Expose
    public double txBandwidth;
    @Expose
    public double rxBandwidth;
    @Expose
    public Map<String, Double> rttMap; // the rrt time to other devices
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

    //// STATUS ABOUT TUPLE OUTPUT RATE AND AVERAGE SIZE
    @Expose
    public Map<Integer, Double> task2Throughput;
    @Expose
    public Map<Integer, Double> task2Delay;
    @Expose
    public Map<Integer, Double>  task2StreamInput;

    public ReportToNimbus(){
        taskID2CPUUsage = new HashMap<Integer,Double>();
        pfThreadName2CPUUsage = new HashMap<String, Double>();
        task2TaskTupleRate = new HashMap<Integer, Map<Integer, Double>>();
        task2TaskTupleAvgSize = new HashMap<Integer, Map<Integer, Double>>();
        rttMap = new HashMap<String, Double>();
        task2Throughput = new HashMap<Integer,Double>();
        task2Delay = new HashMap<Integer,Double>();
        task2StreamInput = new HashMap<Integer, Double>();
    }
}

