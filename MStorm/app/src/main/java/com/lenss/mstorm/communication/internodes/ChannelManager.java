package com.lenss.mstorm.communication.internodes;

import android.util.Pair;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.status.StatusOfDownStreamTasks;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.utils.StatisticsCalculator;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by cmy on 5/12/2016.
 * Updated by cmy on 1/17/2017.
 * Comments: On a computing node, the downstream tasks belonging to the same COMPONENT are regraded as
 *           counterparts; we don't care which task on current node sends output tuple to them. That's
 *           why we use (Component, ChannelGroup) to maintain. However, the upstream tasks are different,
 *           they need the reports from their downstream tasks separately, so we use (Task,ChannelGroup).
 */

public class ChannelManager {
//    private static Map<String, ChannelGroup> component2SendChannels = new ConcurrentHashMap<String, ChannelGroup>();
//    private static Map<String, CopyOnWriteArrayList<Integer>> component2SendChannelIds = new ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>();
//    private static Map<Integer, ChannelGroup> task2RecvChannels = new ConcurrentHashMap<Integer, ChannelGroup>();
//    private static Map<Integer, CopyOnWriteArrayList<Integer>> task2RecvChannelIds = new ConcurrentHashMap<Integer, CopyOnWriteArrayList<Integer>>();
//
//    private static Map<String, CopyOnWriteArrayList<Pair<Channel,Double>>> component2ServerExecutionTimes = new ConcurrentHashMap<String, CopyOnWriteArrayList<Pair<Channel,Double>>>();
//    private static Map<String, CopyOnWriteArrayList<Pair<Channel,Double>>> component2ServerThroughputs = new ConcurrentHashMap<String, CopyOnWriteArrayList<Pair<Channel,Double>>>();
//    private static Map<String, CopyOnWriteArrayList<Pair<Channel,Double>>> component2ServerWaitingQueues = new ConcurrentHashMap<String, CopyOnWriteArrayList<Pair<Channel,Double>>>();
//    private static Map<String, CopyOnWriteArrayList<Pair<Channel,Long>>> component2ServerLastReportUpdateTimes = new ConcurrentHashMap<String, CopyOnWriteArrayList<Pair<Channel,Long>>>();
//

//
//    /** get the channel to the server has the smallest expected waiting time: (L+1-(t-t0)*throughput)*RespT **/
//    private static Channel getSendChannelOnFeedBackExptWaitingTimeMin(String component){
//        // no component yet
//        if(!component2ServerExecutionTimes.keySet().contains(component) ||
//                !component2ServerThroughputs.keySet().contains(component) ||
//                !component2ServerWaitingQueues.keySet().contains(component) ||
//                !component2ServerLastReportUpdateTimes.keySet().contains(component)){
//            return getRandomSendChannel(component);
//        }
//        // begin balance scheduling after every server has feedback
//        if (component2ServerExecutionTimes.get(component).size()<component2SendChannels.get(component).size() ||
//                component2ServerThroughputs.get(component).size()<component2ServerThroughputs.get(component).size() ||
//                component2ServerWaitingQueues.get(component).size()<component2ServerWaitingQueues.get(component).size() ||
//                component2ServerLastReportUpdateTimes.get(component).size()<component2ServerLastReportUpdateTimes.get(component).size()) {
//            return getRandomSendChannel(component);
//        }
//        else{
//            // record the channel with the shortest expected waiting time
//            Long currentTime = SystemClock.elapsedRealtimeNanos();
//
//            int index = 0;
//            double shortestWaitingTime = Double.MAX_VALUE;
//
//            for (int i = 0; i < component2ServerExecutionTimes.get(component).size(); i++) {
//                double exeTimei = component2ServerExecutionTimes.get(component).get(i).second;   //ms
//                //double throughputi = component2ServerThroughputs.get(component).get(i).second;   // throughput + exetime method
//                double waitingQueueLengthi = component2ServerWaitingQueues.get(component).get(i).second;
//                double lastReportTime = component2ServerLastReportUpdateTimes.get(component).get(i).second;
//
//                double elapsedTimei = (currentTime - lastReportTime)/1000000.0;   // ms
//                //double waitingTimei = ((waitingQueueLengthi/throughputi-elapsedTimei)>0) ? ((waitingQueueLengthi+1.0)/throughputi-elapsedTimei)+exeTimei : 1.0/throughputi+exeTimei; // throughput + exetime method
//                double waitingTimei = ((waitingQueueLengthi*exeTimei-elapsedTimei)>0) ? (waitingQueueLengthi*exeTimei-elapsedTimei+exeTimei) : exeTimei;  // exetime method
//
//
//                if (waitingTimei<shortestWaitingTime){
//                    shortestWaitingTime = waitingTimei;
//                    index = i;
//                }
//
//                // update time
//                Pair<Channel,Long> oldReportTime = component2ServerLastReportUpdateTimes.get(component).get(i);
//                Pair<Channel,Long> newReportTime = new Pair<Channel,Long>(oldReportTime.first, currentTime);
//                component2ServerLastReportUpdateTimes.get(component).set(i,newReportTime);
//
//                // update queue
//                Pair<Channel,Double> oldWaitingQueue = component2ServerWaitingQueues.get(component).get(i);
//                //double newQueueLength = ((waitingQueueLengthi-elapsedTimei*throughputi)>0) ? (waitingQueueLengthi-elapsedTimei*throughputi) : 0.0; // throughput + exetime method
//                double newQueueLength = ((waitingQueueLengthi - elapsedTimei/exeTimei)>0) ? (waitingQueueLengthi - elapsedTimei/exeTimei) : 0.0;   // exetime method
//                Pair<Channel,Double> newWaitingQueue = new Pair<Channel,Double>(oldWaitingQueue.first, newQueueLength);
//                component2ServerWaitingQueues.get(component).set(i,newWaitingQueue);
//            }
//
//            Pair<Channel,Double> oldWaitingQueue = component2ServerWaitingQueues.get(component).get(index);
//            Pair<Channel,Double> newWaitingQueue = new Pair<Channel,Double>(oldWaitingQueue.first,oldWaitingQueue.second+1.0);
//            component2ServerWaitingQueues.get(component).set(index,newWaitingQueue);
//
//            Channel ch = component2ServerExecutionTimes.get(component).get(index).first;
//            return ch;
//        }
//    }

    private static final String TAG="ChannelManager";
    private static Logger logger = Logger.getLogger(TAG);

    public static Map<Integer, Channel> availRemoteTask2Channel = new ConcurrentHashMap<>();
    public static Map<String, CopyOnWriteArrayList<Integer>> comp2AvailRemoteTasks = new ConcurrentHashMap<>();
    public static Map<Integer, String> channel2RemoteGUID = new ConcurrentHashMap<>();
    public static Map<String, String> tempIP2GUID = new ConcurrentHashMap<>(); // used for mapping from remote IP to remote GUID at reconnecting time

    public static void addChannelToRemote(Channel ch, String remoteGUID){
        channel2RemoteGUID.put(ch.getId(), remoteGUID);
        Assignment assignment = ComputingNode.getAssignment();
        if (assignment != null) {
            HashMap<String, ArrayList<Integer>> node2tasks = assignment.getNode2Tasks();
            HashMap<Integer, String> task2Comp = assignment.getTask2Component();
            ArrayList<Integer> remoteTasks = node2tasks.get(remoteGUID);
            for (Integer remoteTask : remoteTasks) {
                availRemoteTask2Channel.put(remoteTask, ch);
                String comp = task2Comp.get(remoteTask);
                if (comp2AvailRemoteTasks.containsKey(comp)) {
                    if(!comp2AvailRemoteTasks.get(comp).contains(remoteTask)) {
                        comp2AvailRemoteTasks.get(comp).add(remoteTask);
                    }
                } else {
                    CopyOnWriteArrayList<Integer> availTasks = new CopyOnWriteArrayList<>();
                    availTasks.add(remoteTask);
                    comp2AvailRemoteTasks.put(comp, availTasks);
                }
                StatusOfDownStreamTasks.initDownStreamTaskLink(remoteTask);
            }
        }
    }

    public static void removeChannelToRemote(Channel ch){
        String remoteGUID = channel2RemoteGUID.get(ch.getId());
        Assignment assignment = ComputingNode.getAssignment();
        if(assignment!=null){
            HashMap<String, ArrayList<Integer>> node2tasks = assignment.getNode2Tasks();
            HashMap<Integer, String> task2Comp = assignment.getTask2Component();
            List<Integer> remoteTasks = node2tasks.get(remoteGUID);
            for(Integer remoteTask: remoteTasks){
                availRemoteTask2Channel.remove(remoteTask);
                String comp = task2Comp.get(remoteTask);
                if(comp2AvailRemoteTasks.containsKey(comp)){
                    comp2AvailRemoteTasks.get(comp).remove(remoteTask);
                }
                StatusOfDownStreamTasks.removeReport(remoteTask);
            }
        }
        channel2RemoteGUID.remove(ch.getId());
    }

    public static void removeChannelToRemoteGUID(Channel ch) {
        channel2RemoteGUID.remove(ch.getId());
    }

    public static void releaseChannelsToRemote(){
        Iterator<Map.Entry<Integer,Channel>> it1 = availRemoteTask2Channel.entrySet().iterator();
        while(it1.hasNext()){
            Map.Entry<Integer,Channel> entry1 = it1.next();
            entry1.getValue().disconnect();
            entry1.getValue().close();
        }
        availRemoteTask2Channel.clear();
        comp2AvailRemoteTasks.clear();
        channel2RemoteGUID.clear();
        StatusOfDownStreamTasks.removeAllStatus();
    }

    public static DispatchStatus sendToRandomDownstreamTask(String component, InternodePacket pkt){
        DispatchStatus dispatchStatus = new DispatchStatus();
        List<Integer> availTasks = comp2AvailRemoteTasks.get(component);
        if(availTasks == null || (availTasks!=null && availTasks.size()==0)){
            dispatchStatus.remoteTaskID = -1;
            dispatchStatus.success = false;
        } else {
            int index = Helper.randInt(0, availTasks.size());
            int taskID = availTasks.get(index);
            Channel ch = availRemoteTask2Channel.get(taskID);
            logger.info("------- SendToRandomDownStream ------, from " + pkt.fromTask + "-------------------");
            if (ch != null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = true;
                logger.info("------- SendToRandomDownStream ------, to " + taskID + "-------------------");
            } else {
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = false;
                logger.info("------- SendToRandomDownStream ------, to " + taskID + " failed -------------------");
            }
        }
        return dispatchStatus;
    }

    public static DispatchStatus sendToDownstreamTaskMinSojournTime(String component, InternodePacket pkt, boolean localRequired){
        DispatchStatus dispatchStatus;
        List<Integer> availableTasks = comp2AvailRemoteTasks.get(component);
        Set<Integer> availableTasksWithStatus = new HashSet<>(availableTasks);
        availableTasksWithStatus.retainAll(StatusOfDownStreamTasks.taskID2SojournTime.keySet());
        availableTasksWithStatus.retainAll(StatusOfDownStreamTasks.taskID2LinkQuality.keySet());

        if(availableTasksWithStatus.size()==0){  // no remote task status yet
            dispatchStatus = sendToRandomDownstreamTask(component, pkt);
        } else {
            dispatchStatus = new DispatchStatus();
            double minSojournTime = Double.MAX_VALUE;
            int taskID = -1;

            logger.info("------ SendToMinSojournTime------, from " + pkt.fromTask + " -----------------");
            for(int availableTaskID: availableTasksWithStatus){
                double sojounTime = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTaskID);
                double linkQuality = StatusOfDownStreamTasks.taskID2LinkQuality.get(availableTaskID);
                sojounTime = sojounTime/linkQuality;
                logger.info("TaskID: " + availableTaskID + " LQ: " + linkQuality + " SojournTime:" + sojounTime);
                if(localRequired){
                    if((sojounTime < minSojournTime) && Supervisor.newAssignment.getTask2Node().get(availableTaskID).equals(MStorm.GUID)){
                        minSojournTime = sojounTime;
                        taskID = availableTaskID;
                    }
                } else {
                    if(sojounTime < minSojournTime){
                        minSojournTime = sojounTime;
                        taskID = availableTaskID;
                    }
                }
            }

            Channel ch = availRemoteTask2Channel.get(taskID);
            if (ch!=null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = true;
                logger.info("------ SendToMinSojournTime------, to " + taskID + " -----------------");
            } else {
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = false;
                logger.info("------ SendToMinSojournTime------, to " + taskID + " failed -----------------");
            }
        }
        return dispatchStatus;
    }

    /********************************************************************************************
     *|--sojourn1/sojourn1--|--sojourn1/sojourn2--|-------- ... ---------|--sojourn1/sojournN--|*
     *          0                     1                     ...                   N-1           *
     *******************************************************************************************/
    public static DispatchStatus sendToDownstreamTaskSojournTimeProb(String component, InternodePacket pkt, boolean localRequired){
        DispatchStatus dispatchStatus;
        List<Integer> availableTasks = comp2AvailRemoteTasks.get(component);
        Set<Integer> availableTasksWithStatus = new HashSet<>(availableTasks);
        availableTasksWithStatus.retainAll(StatusOfDownStreamTasks.taskID2SojournTime.keySet());
        availableTasksWithStatus.retainAll(StatusOfDownStreamTasks.taskID2LinkQuality.keySet());

        if(availableTasksWithStatus.size()==0){  // no downstream task has report yet
            dispatchStatus = sendToRandomDownstreamTask(component, pkt);
        } else {
            dispatchStatus = new DispatchStatus();
            // unify the sojourn time based on sojourn1 and added to a probability slot
            ArrayList<Pair<Integer,Double>> sojournTimeBasedProbSlots = new ArrayList<>();
            ArrayList<Integer> availableTaskListWithStatus = new ArrayList<>(availableTasksWithStatus);
            double sojourn1 = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTaskListWithStatus.get(0));
            double maxBound = 0.0;

            logger.info("------ SendToSojournTimeProb------, from " + pkt.fromTask + " -----------------");
            for(int availableTaskID: availableTaskListWithStatus){
                if(localRequired){
                    if(Supervisor.newAssignment.getTask2Node().get(availableTaskID).equals(MStorm.GUID)){
                        double sojournTime = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTaskID);
                        double linkQuality = StatusOfDownStreamTasks.taskID2LinkQuality.get(availableTaskID);
                        sojournTime = sojournTime / linkQuality;
                        double unifiedSojournTime = sojourn1/sojournTime;
                        sojournTimeBasedProbSlots.add(new Pair<>(availableTaskID,unifiedSojournTime));
                        maxBound += unifiedSojournTime;
                    }
                } else {
                    double sojournTime = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTaskID);
                    double linkQuality = StatusOfDownStreamTasks.taskID2LinkQuality.get(availableTaskID);
                    sojournTime = sojournTime / linkQuality;
                    double unifiedSojournTime = sojourn1/sojournTime;
                    sojournTimeBasedProbSlots.add(new Pair<>(availableTaskID,unifiedSojournTime));
                    maxBound += unifiedSojournTime;
                }
            }
            // find the specific channel based probability
            double randomDouble = Helper.randDouble(0,maxBound);
            int selectedIndex = 0;
            for (int i = 0; i < sojournTimeBasedProbSlots.size(); i++) {
                double slotTime = sojournTimeBasedProbSlots.get(i).second;
                if (randomDouble < slotTime) {
                    selectedIndex = i;
                    break;
                } else {
                    randomDouble -= slotTime;
                }
            }
            int taskID = sojournTimeBasedProbSlots.get(selectedIndex).first;

            Channel ch = availRemoteTask2Channel.get(taskID);
            if (ch!=null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = true;
                logger.info("------ SendToSojournTimeProb------, to " + taskID + " -----------------");
            } else {
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = false;
                logger.info("------ SendToSojournTimeProb------, to " + taskID + " failed -----------------");
            }
        }
        return dispatchStatus;
    }

    public static DispatchStatus sendToDownstreamTaskMinEWT(String component, InternodePacket pkt, boolean localRequired) {
        DispatchStatus dispatchStatus;
        List<Integer> availableTasks = comp2AvailRemoteTasks.get(component);
        Set<Integer> availableTasksWithStatus = new HashSet<>(availableTasks);
        availableTasksWithStatus.retainAll(StatusOfDownStreamTasks.taskID2InQueueLength.keySet());
        availableTasksWithStatus.retainAll(StatusOfDownStreamTasks.taskID2LinkQuality.keySet());

        if(availableTasksWithStatus.size()==0){
            dispatchStatus = sendToRandomDownstreamTask(component, pkt);
        } else {
            dispatchStatus = new DispatchStatus();
            double minEWT = Double.MAX_VALUE;
            int taskID = -1;

            logger.info("------ SendToMinEWT------, from " + pkt.fromTask + " -----------------");
            for(int availableTaskID: availableTasksWithStatus){
                double linkQuality = StatusOfDownStreamTasks.taskID2LinkQuality.get(availableTaskID);
                double inQueueLength = StatusOfDownStreamTasks.taskID2InQueueLength.get(availableTaskID);
                inQueueLength = (inQueueLength==0) ? StatisticsCalculator.SMALL_VALUE : inQueueLength;
                double outQueueLength = StatusOfDownStreamTasks.taskID2OutQueueLength.get(availableTaskID);
                outQueueLength = (outQueueLength==0)? StatisticsCalculator.SMALL_VALUE : outQueueLength;

//                // version 1.0
//                double inputRate = StatusOfDownStreamTasks.taskID2InputRate.get(availableTaskID);
//                double procRate = StatusOfDownStreamTasks.taskID2ProcRate.get(availableTaskID);
//                double outputRate = StatusOfDownStreamTasks.taskID2OutputRate.get(availableTaskID);
//                double WT = (inQueueLength/procRate > outQueueLength/outputRate) ? inQueueLength/procRate : outQueueLength/outputRate;
//                double EWT = WT /linkQuality;
//                logger.info("TaskID: " + availableTaskID + " LQ: " + linkQuality + " IQL: " + inQueueLength + " OQL: " +
//                        outQueueLength + " INR: " + inputRate + " PROCR: " + procRate + " OUTR: " + outputRate + " WT: " + WT + " EWT:" + EWT);

                // version 2.0
                double procRate = StatusOfDownStreamTasks.taskID2ProcRate.get(availableTaskID);
                procRate = (procRate==0)? StatisticsCalculator.SMALL_VALUE:procRate;
                double EWT = inQueueLength * outQueueLength / linkQuality / procRate;
                logger.info("TaskID: " + availableTaskID + " LQ: " + linkQuality + " IQL: " + inQueueLength + " OQL: " +  outQueueLength + " PROCR: " + procRate + " EWT:" + EWT);

                if(localRequired){
                    if((EWT < minEWT) && Supervisor.newAssignment.getTask2Node().get(availableTaskID).equals(MStorm.GUID)){
                        minEWT = EWT;
                        taskID = availableTaskID;
                    }
                } else {
                    if(EWT < minEWT){
                        minEWT = EWT;
                        taskID = availableTaskID;
                    }
                }
            }
            Channel ch = availRemoteTask2Channel.get(taskID);
            if (ch!=null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = true;
                logger.info("------ SendToMinEWT------, to " + taskID + "  -----------------");
            } else {
                dispatchStatus.remoteTaskID = taskID;
                dispatchStatus.success = false;
                logger.info("------ SendToMinEWT------, to " + taskID + " failed -----------------");
            }
        }
        return dispatchStatus;
    }

    public static int broadcastToUpstreamTasks(int taskID, InternodePacket pkt) {
        Assignment assignment = ComputingNode.getAssignment();
        String comp = assignment.getTask2Component().get(taskID);
        Topology topology = ComputingNode.getTopology();
        ArrayList<String> upstreamComps = topology.getUpStreamComponents(comp);
        ChannelGroup broadcastChannels = new DefaultChannelGroup(comp);
        for(String upstreamComp: upstreamComps) {
            List<Integer> upstreamTasks = comp2AvailRemoteTasks.get(upstreamComp);
            for(int upstreamTask: upstreamTasks){
                Channel ch = availRemoteTask2Channel.get(upstreamTask);
                if(ch!=null)
                    broadcastChannels.add(ch);
            }
        }
        if(broadcastChannels.size()!=0){
            broadcastChannels.write(pkt);
            return 0;
        }
        else
            return -1;
    }

    public static int broadcastToDownstreamTasks(int taskID, InternodePacket pkt) {
        Assignment assignment = ComputingNode.getAssignment();
        String comp = assignment.getTask2Component().get(taskID);
        ChannelGroup broadcastChannels = new DefaultChannelGroup(comp);
        List<Integer> downstreamTasks = comp2AvailRemoteTasks.get(comp);
        for(int downstreamTask: downstreamTasks){
            Channel ch = availRemoteTask2Channel.get(downstreamTask);
            broadcastChannels.add(ch);
        }
        if(broadcastChannels.size()!=0) {
            broadcastChannels.write(pkt);
            return 0;
        }
        else
            return -1;
    }
}
