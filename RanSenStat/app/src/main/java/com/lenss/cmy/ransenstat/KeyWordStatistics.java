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
public class KeyWordStatistics extends Processor {
    @Expose
    private int workload_SenStat;

    public void setWorkload(int workload){
        workload_SenStat = workload;
    }

    @Override
    public void execute() {
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
                if (pktRecv != null) {
                    long enterTime = SystemClock.elapsedRealtimeNanos();
                    Utils.fakeExecutionTime(workload_SenStat);
                    InternodePacket pktSend = new InternodePacket();
                    pktSend.ID = pktRecv.ID;
                    pktSend.type = InternodePacket.TYPE_DATA;
                    pktSend.fromTask = taskID;
                    pktSend.simpleContent = pktRecv.simpleContent;
                    pktSend.traceTask = pktRecv.traceTask;
                    pktSend.traceTask.add("KS_"+taskID);
                    pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                    pktSend.traceTaskEnterTime.put("KS_"+ taskID, enterTime);
                    pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                    long exitTime = SystemClock.elapsedRealtimeNanos();
                    pktSend.traceTaskExitTime.put("KS_"+ taskID, exitTime);
                    String component = SentenceSink.class.getName();
                    MessageQueues.emit(pktSend, taskID, component);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
