package com.lenss.mstorm.topology;

import java.io.Serializable;

public abstract  class BTask implements Serializable {
    private String component;
    private int taskID;
    private String sourceIP;

    public abstract void prepare();

    public abstract void execute();

    public abstract void postExecute();

    public void setTaskID(int id) {
        taskID=id;
    }

    public int getTaskID() {
        return taskID;
    }

    public void setComponent(String component) {
        this.component=component;
    }

    public String getComponent() {
        return component;
    }

    public void setSourceIP(String IP){
        sourceIP = IP;
    }

    public String getSourceIP(){
        return sourceIP;
    }
}
