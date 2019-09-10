package com.lenss.mstorm.communication.internodes;

import android.util.Pair;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.status.StatusOfDownStreamTasks;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Helper;
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
                    comp2AvailRemoteTasks.get(comp).add(remoteTask);
                } else {
                    CopyOnWriteArrayList<Integer> availTasks = new CopyOnWriteArrayList<>();
                    availTasks.add(remoteTask);
                    comp2AvailRemoteTasks.put(comp, availTasks);
                }
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

    public static int sendToRandomDownstreamTask(String component, InternodePacket pkt){
        List<Integer> availTasks = comp2AvailRemoteTasks.get(component);
        if(availTasks == null || (availTasks!=null && availTasks.size()==0)){
            return -1;
        } else {
            int index = Helper.randInt(0, availTasks.size());
            int taskID = availTasks.get(index);
            Channel ch = availRemoteTask2Channel.get(taskID);
            if (ch != null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                return taskID;
            } else {
                return -1;
            }
        }
    }

    public static int sendToDownstreamTaskMinSojournTime(String component, InternodePacket pkt){
        int taskID = -1;
        CopyOnWriteArrayList<Integer> availableTasks = comp2AvailRemoteTasks.get(component);
        HashSet<Integer> availableTaskSet = new HashSet<>(availableTasks);

        if(!StatusOfDownStreamTasks.taskID2SojournTime.keySet().containsAll(availableTaskSet)){  // not every downstream task has report yet
            taskID = sendToRandomDownstreamTask(component, pkt);
            return taskID;
        } else {
            double minSojournTIme = Double.MAX_VALUE;
            for(int availableTaskID: availableTasks){
                double sojounTime = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTaskID);
                if(sojounTime < minSojournTIme){
                    minSojournTIme = sojounTime;
                    taskID = availableTaskID;
                }
            }
            Channel ch = availRemoteTask2Channel.get(taskID);
            if (ch!=null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                return taskID;
            } else {
                return -1;
            }
        }
    }

    /********************************************************************************************
     *|--sojourn1/sojourn1--|--sojourn1/sojourn2--|-------- ... ---------|--sojourn1/sojournN--|*
     *          0                     1                     ...                   N-1           *
     *******************************************************************************************/
    public static int sendToDownstreamTaskMinSojournTimeProb(String component, InternodePacket pkt){
        int taskID = -1;
        CopyOnWriteArrayList<Integer> availableTasks = comp2AvailRemoteTasks.get(component);
        HashSet<Integer> availableTaskSet = new HashSet<>(availableTasks);

        if(!StatusOfDownStreamTasks.taskID2SojournTime.keySet().containsAll(availableTaskSet)){  // not every downstream task has report yet
            taskID = sendToRandomDownstreamTask(component, pkt);
            return taskID;
        } else {
            // unify the sojourn time based on sojourn1 and added to a probability slot
            ArrayList<Pair<Integer,Double>> sojournTimeBasedProbSlots = new ArrayList<>();
            double sojourn1 = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTasks.get(0));
            double maxBound = 0.0;
            for(int availableTaskID: availableTasks){
                double sojournTime = StatusOfDownStreamTasks.taskID2SojournTime.get(availableTaskID);
                double unifiedSojournTime = sojourn1/sojournTime;
                sojournTimeBasedProbSlots.add(new Pair<>(availableTaskID,unifiedSojournTime));
                maxBound += unifiedSojournTime;
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
            taskID = sojournTimeBasedProbSlots.get(selectedIndex).first;
            Channel ch = availRemoteTask2Channel.get(taskID);
            if (ch!=null && ch.isWritable()) {
                pkt.toTask = taskID;
                ch.write(pkt);
                return taskID;
            } else {
                return -1;
            }
        }
    }

    public static int sendToDownstreamTaskMinSojournTimeFineGrained(String component, InternodePacket pkt) {
        int taskID = -1;
        CopyOnWriteArrayList<Integer> availableTasks = comp2AvailRemoteTasks.get(component);
        HashSet<Integer> availableTaskSet = new HashSet<>(availableTasks);

        if(!StatusOfDownStreamTasks.taskID2SojournTime.keySet().containsAll(availableTaskSet)){  // not every downstream task has report yet
            taskID = sendToRandomDownstreamTask(component, pkt);
            return taskID;
        } else {
            //todo
            return taskID;
        }
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
