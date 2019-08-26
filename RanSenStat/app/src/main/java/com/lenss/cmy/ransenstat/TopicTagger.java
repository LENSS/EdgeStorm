package com.lenss.cmy.ransenstat;


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
                InternodePacket pkt = MessageQueues.retrieveIncomingQueue(taskID);
                if (pkt != null) {
                    Utils.fakeExecutionTime(workLoad_TopicTagger);
                    String component = KeyWordStatistics.class.getName();
                    pkt.type = InternodePacket.TYPE_DATA;
                    pkt.fromTask = taskID;
                    MessageQueues.emit(pkt,taskID,component);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
