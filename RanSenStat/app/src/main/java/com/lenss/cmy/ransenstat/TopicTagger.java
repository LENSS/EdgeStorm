package com.lenss.cmy.ransenstat;


import android.os.SystemClock;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;
import tools.Utils;

/**
 * Created by cmy on 6/23/16.
 */
public class TopicTagger extends Processor {
    @Expose
    private int workLoad_TopicTagger;

    public void setWorkload(int workload){this.workLoad_TopicTagger = workload;}

    @Override
    public void execute() {
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
                if (pktRecv != null) {
                    long enterTime = SystemClock.elapsedRealtimeNanos();
                    Utils.fakeExecutionTime(workLoad_TopicTagger);
                    InternodePacket pktSend = new InternodePacket();
                    pktSend.ID = pktRecv.ID;
                    pktSend.type = InternodePacket.TYPE_DATA;
                    pktSend.fromTask = taskID;
                    pktSend.simpleContent = pktRecv.simpleContent;
                    pktSend.traceTask = pktRecv.traceTask;
                    pktSend.traceTask.add("TT_"+taskID);
                    pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                    pktSend.traceTaskEnterTime.put("TT_"+ taskID, enterTime);
                    pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                    long exitTime = SystemClock.elapsedRealtimeNanos();
                    pktSend.traceTaskExitTime.put("TT_"+ taskID, exitTime);
                    String component = KeyWordStatistics.class.getName();
                    MessageQueues.emit(pktSend,taskID,component);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
