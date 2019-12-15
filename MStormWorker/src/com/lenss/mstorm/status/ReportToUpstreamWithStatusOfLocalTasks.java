package com.lenss.mstorm.status;

import com.google.gson.annotations.Expose;

import java.util.HashMap;
import java.util.Map;

public class ReportToUpstreamWithStatusOfLocalTasks {
    // Input rate
    @Expose
    public Map<Integer, Double> task2InputRate = new HashMap<Integer, Double>();

    // Output rate
    @Expose
    public Map<Integer, Double> task2OutputRate = new HashMap<Integer, Double>();

    // Processing rate
    @Expose
    public Map<Integer, Double> task2ProcRate = new HashMap<Integer, Double>();

    // Sojourn time
    @Expose
    public Map<Integer, Double> task2SojournTime = new HashMap<Integer, Double>();

    // Input queue
    @Expose
    public Map<Integer, Double> task2InQueueLength = new HashMap<Integer, Double>();

    // Output queue
    @Expose
    public Map<Integer, Double> task2OutQueueLength = new HashMap<Integer, Double>();
}
