package com.lenss.mstorm.status;

import com.google.gson.annotations.Expose;

public class ReportToUpstreamWithStatusOfSingleLocalTask {
    @Expose
    public int taskID;

    @Expose
    public double inputRate;

    @Expose
    public double outputRate;

    @Expose
    public double procRate;

    @Expose
    public double sojournTime;

    @Expose
    public double inQueueLength;

    @Expose
    public double outQueueLength;
}
