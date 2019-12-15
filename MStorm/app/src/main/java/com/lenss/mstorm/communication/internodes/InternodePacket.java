package com.lenss.mstorm.communication.internodes;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cmy on 7/30/19.
 */

public class InternodePacket {
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

    public int pktSize(){
        // head size
        int size = 20;

        // trace track size
        for(String task: traceTask){
            size += task.length();
        }

        // trace track entry and exit time size
        for(Map.Entry<String,Long> entry: traceTaskEnterTime.entrySet()){
            size += (entry.getKey().length() + 8) * 2;
        }

        // simple content size
        for(Map.Entry<String,String> entry: simpleContent.entrySet()){
            size += entry.getKey().length() + entry.getValue().length();
        }

        // complex content size
        size += (complexContent==null) ? 0 : complexContent.length;

        return size;
    }
}
