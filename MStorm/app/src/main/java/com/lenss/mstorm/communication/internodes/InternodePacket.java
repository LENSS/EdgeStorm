package com.lenss.mstorm.communication.internodes;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by cmy on 7/30/19.
 */

public class InternodePacket implements Serializable {
    public static final int TYPE_INIT = 0;
    public static final int TYPE_DATA = 1;
    public static final int TYPE_REPORT = 2;
    public static final int TYPE_ACK = 3;

    @Expose
    public long ID;         // unique ID for a tuple, valid for data and report packet
    @Expose
    public int type;        // supporting three types: data, report, acknowledgement
    @Expose
    public int fromTask;    // upstream taskID
    @Expose
    public int toTask;      // downStream taskID
    @Expose
    public ArrayList<String> traceTask;   // trace logical stream path
    @Expose
    public HashMap<String, Long> traceTaskEnterTime;    // trace the time entering a task
    @Expose
    public HashMap<String, Long> traceTaskExitTime;     // trace the time exiting a task
    @Expose
    public HashMap<String, String> simpleContent;       // storing simple content
    @Expose
    public byte[] complexContent;                       // storing complex content, such as a picture frame

    public InternodePacket(){
        traceTask = new ArrayList<>();
        traceTaskEnterTime = new HashMap<>();
        traceTaskExitTime = new HashMap<>();
        simpleContent = new HashMap<>();
    }
}
