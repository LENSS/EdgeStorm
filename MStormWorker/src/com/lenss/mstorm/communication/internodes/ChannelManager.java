package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.utils.MyPair;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

    private static Map<String, ChannelGroup> component2SendChannels = new ConcurrentHashMap<String, ChannelGroup>();
    private static Map<String, CopyOnWriteArrayList<Integer>> component2SendChannelIds = new ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>();
    private static Map<Integer, ChannelGroup> task2RecvChannels = new ConcurrentHashMap<Integer, ChannelGroup>();
    private static Map<Integer, CopyOnWriteArrayList<Integer>> task2RecvChannelIds = new ConcurrentHashMap<Integer, CopyOnWriteArrayList<Integer>>();
    private static Map<String, CopyOnWriteArrayList<MyPair<Channel,Double>>> component2ServerExecutionTimes = new ConcurrentHashMap<String, CopyOnWriteArrayList<MyPair<Channel,Double>>>();
    private static Map<String, CopyOnWriteArrayList<MyPair<Channel,Double>>> component2ServerThroughputs = new ConcurrentHashMap<String, CopyOnWriteArrayList<MyPair<Channel,Double>>>();
    private static Map<String, CopyOnWriteArrayList<MyPair<Channel,Double>>> component2ServerWaitingQueues = new ConcurrentHashMap<String, CopyOnWriteArrayList<MyPair<Channel,Double>>>();
    private static Map<String, CopyOnWriteArrayList<MyPair<Channel,Long>>> component2ServerLastReportUpdateTimes = new ConcurrentHashMap<String, CopyOnWriteArrayList<MyPair<Channel,Long>>>();

    public static void releaseChannels(){
        Iterator<Map.Entry<String, ChannelGroup>> it1 = component2SendChannels.entrySet().iterator();
        while(it1.hasNext()){
            Map.Entry<String, ChannelGroup> entry1 = it1.next();
            entry1.getValue().disconnect();
            entry1.getValue().close();
        }
        Iterator<Map.Entry<Integer, ChannelGroup>> it2 = task2RecvChannels.entrySet().iterator();
        while(it2.hasNext()){
            Map.Entry<Integer, ChannelGroup> entry2 = it2.next();
            entry2.getValue().disconnect();
            entry2.getValue().close();
        }

        component2SendChannels.clear();
        component2SendChannelIds.clear();
        task2RecvChannels.clear();
        task2RecvChannelIds.clear();

        component2ServerExecutionTimes.clear();
        component2ServerThroughputs.clear();
        component2ServerWaitingQueues.clear();
        component2ServerLastReportUpdateTimes.clear();

        System.out.println("The channel has been released successfully ... ");
    }

    private static void write(byte[] data, Channel ch){
        if (ch.isWritable()){
            ch.write(data);
        }
    }

    /** record where the client send computation task to and receive report from **/
    public static synchronized void addComponent2SendChannels(String component, Channel ch){
        if (component2SendChannels.keySet().contains(component)) {
            component2SendChannels.get(component).add(ch);
        } else{
            ChannelGroup sendChannels= new DefaultChannelGroup(component);
            sendChannels.add(ch);
            component2SendChannels.put(component,sendChannels);
        }

        if(component2SendChannelIds.keySet().contains(component)) {
            component2SendChannelIds.get(component).add(ch.getId());
        } else{
            CopyOnWriteArrayList<Integer> sendChannelIds = new CopyOnWriteArrayList<Integer>();
            sendChannelIds.add(ch.getId());
            component2SendChannelIds.put(component, sendChannelIds);
        }
    }

    /** record where the server receive computation from and send report to **/
    public static void addTask2RecvChannels(int taskID, Channel ch){
        if (task2RecvChannels.keySet().contains(taskID))
            task2RecvChannels.get(taskID).add(ch);
        else{
            ChannelGroup recvChannels= new DefaultChannelGroup(Integer.toString(taskID));
            recvChannels.add(ch);
            task2RecvChannels.put(taskID,recvChannels);
        }

        if(task2RecvChannelIds.keySet().contains(taskID)) {
            task2RecvChannelIds.get(taskID).add(ch.getId());
        } else{
            CopyOnWriteArrayList<Integer> recvChannelIds = new CopyOnWriteArrayList<Integer>();
            recvChannelIds.add(ch.getId());
            task2RecvChannelIds.put(taskID,recvChannelIds);
        }
    }

    /** broadcast computation task to servers that the client is currently connected to **/
    public static int broadcastToServers(String component, byte[] gop) {
        ChannelGroup sendChannels = component2SendChannels.get(component);
        if(sendChannels.size()!=0) {
            sendChannels.write(gop);
            return 0;
        }
        else
            return -1;
    }

    /** broadcast report to the clients that the server currently connected to **/
    public static int broadcastToClients(int taskID, byte[] report) {
        ChannelGroup recvChannels = task2RecvChannels.get(taskID);
        if(recvChannels!=null && recvChannels.size()!=0){
            recvChannels.write(report);
            return 0;
        }
        else
            return -1;
    }

    /** send computation task to random server **/
    public static int sendToRandomComponentServer(String component, byte[] gop){
        Channel ch = getRandomSendChannel(component);
        if (ch!=null) {
            HashMap<Integer, MyPair<Integer, Integer>> port2TaskPair = ComputingNode.getAssignment().getPort2TaskPair();
            int port = ((InetSocketAddress) ch.getLocalAddress()).getPort();
            int remoteTaskID = port2TaskPair.get(port).right;
            write(gop, ch);
            return remoteTaskID;
        }
        else {
            return -1;
        }
    }

    /** get the channel to a random server **/
    private static Channel getRandomSendChannel (String component){
        ChannelGroup sendChannels = component2SendChannels.get(component);
        CopyOnWriteArrayList<Integer> sendChannelIds = component2SendChannelIds.get(component);
        if (!sendChannelIds.isEmpty()) {
            int randIndex = Helper.randInt(0, sendChannelIds.size() - 1);
            int chId = sendChannelIds.get(randIndex);
            return sendChannels.find(chId);
        }
        else
        {
            return null;
        }
    }

    /** send computation task to server based on feedback execution time **/
    public static int sendToComponentServerOnFeedBackExeTime(String component, byte[] gop) {
        Channel ch = getSendChannelOnFeedBackExptWaitingTimeMin(component);
        //Channel ch = getSendChannelOnFeedBackExeTimeMin(component);
        //Channel ch = getSendChannelOnFeedBackExeTimeProb(component);
        if (ch != null){
            HashMap<Integer, MyPair<Integer, Integer>> port2TaskPair = ComputingNode.getAssignment().getPort2TaskPair();
            int port = ((InetSocketAddress) ch.getLocalAddress()).getPort();
            int remoteTaskID = port2TaskPair.get(port).right;
            write(gop, ch);
            return remoteTaskID;
        }
        else{
            return -1;
        }
    }

    /*************************************************************************************************************
     *  |-----1-----|--exeTime1/exeTime2--|----exeTime1/exeTime3-|-----....-------|----exeTime1/exeTimeN------|   *
     *  0           1                     2   RandomIndex        3                N                               *
     *************************************************************************************************************/
    private static Channel getSendChannelOnFeedBackExeTimeProb(String component) {
        ArrayList<MyPair<Channel,Double>> exeTimeBasedProbSlots = new ArrayList<MyPair<Channel,Double>>();
        // no component yet
        if(!component2ServerExecutionTimes.keySet().contains(component)){
            return getRandomSendChannel(component);
        }

        // begin balance scheduling after every server has feedback
        if (component2ServerExecutionTimes.get(component).size()<component2SendChannels.get(component).size())
            return getRandomSendChannel(component);
        else {
            // generate the first slot
            Channel ch = component2ServerExecutionTimes.get(component).get(0).left;
            double baseTime = component2ServerExecutionTimes.get(component).get(0).right;
            double unifiedTime = 1;
            exeTimeBasedProbSlots.add(new MyPair(ch, unifiedTime));
            // generate the following slots
            for (int i = 1; i < component2ServerExecutionTimes.get(component).size(); i++) {
                ch = component2ServerExecutionTimes.get(component).get(i).left;
                unifiedTime = baseTime/component2ServerExecutionTimes.get(component).get(i).right + exeTimeBasedProbSlots.get(i - 1).right;
                exeTimeBasedProbSlots.add(new MyPair(ch, unifiedTime));
            }
        }

        // choose channel according to the slot which a randomDouble falls into
        double maxBound = exeTimeBasedProbSlots.get(exeTimeBasedProbSlots.size()-1).right;
        double randomDouble = Helper.randDouble(0,maxBound);
        int randomIndex = 0;
        for (int i = 0; i < exeTimeBasedProbSlots.size();i++)
        {
            if (randomDouble<exeTimeBasedProbSlots.get(i).right)
            {
                randomIndex = i;
                break;
            }
        }
        Channel selectedChannel = exeTimeBasedProbSlots.get(randomIndex).left;

        return selectedChannel;
    }

    /** get the channel to the server executes tasks fastest **/
    private static Channel getSendChannelOnFeedBackExeTimeMin(String component) {
        // no component yet
        if(!component2ServerExecutionTimes.keySet().contains(component)){
            return getRandomSendChannel(component);
        }

        // begin balance scheduling after every server has feedback
        if (component2ServerExecutionTimes.get(component).size()<component2SendChannels.get(component).size())
            return getRandomSendChannel(component);
        else {
            // record the channel with the shortest processing time
            Channel ch = component2ServerExecutionTimes.get(component).get(0).left;
            double shortTime = component2ServerExecutionTimes.get(component).get(0).right;
            for (int i = 1; i < component2ServerExecutionTimes.get(component).size(); i++) {
                Channel chi = component2ServerExecutionTimes.get(component).get(i).left;
                double timei = component2ServerExecutionTimes.get(component).get(i).right;
                if (timei<shortTime){
                    shortTime = timei;
                    ch = chi;
                }
            }
            return ch;
        }
    }

    /** get the channel to the server has the smallest expected waiting time: (L+1-(t-t0)*throughput)*RespT **/
    private static Channel getSendChannelOnFeedBackExptWaitingTimeMin(String component){
        // no component yet
        if(!component2ServerExecutionTimes.keySet().contains(component) ||
              !component2ServerThroughputs.keySet().contains(component) ||
              !component2ServerWaitingQueues.keySet().contains(component) ||
              !component2ServerLastReportUpdateTimes.keySet().contains(component)){
            return getRandomSendChannel(component);
        }
        // begin balance scheduling after every server has feedback
        if (component2ServerExecutionTimes.get(component).size()<component2SendChannels.get(component).size() ||
                component2ServerThroughputs.get(component).size()<component2ServerThroughputs.get(component).size() ||
                component2ServerWaitingQueues.get(component).size()<component2ServerWaitingQueues.get(component).size() ||
                component2ServerLastReportUpdateTimes.get(component).size()<component2ServerLastReportUpdateTimes.get(component).size()) {
            return getRandomSendChannel(component);
        }
        else{
            // record the channel with the shortest expected waiting time
            Long currentTime = System.nanoTime();

            int index = 0;
            double shortestWaitingTime = Double.MAX_VALUE;

            for (int i = 0; i < component2ServerExecutionTimes.get(component).size(); i++) {
                double exeTimei = component2ServerExecutionTimes.get(component).get(i).right;   //ms
                //double throughputi = component2ServerThroughputs.get(component).get(i).right;   // throughput + exetime method
                double waitingQueueLengthi = component2ServerWaitingQueues.get(component).get(i).right;
                double lastReportTime = component2ServerLastReportUpdateTimes.get(component).get(i).right;

                double elapsedTimei = (currentTime - lastReportTime)/1000000.0;   // ms
                //double waitingTimei = ((waitingQueueLengthi/throughputi-elapsedTimei)>0) ? ((waitingQueueLengthi+1.0)/throughputi-elapsedTimei)+exeTimei : 1.0/throughputi+exeTimei; // throughput + exetime method
                double waitingTimei = ((waitingQueueLengthi*exeTimei-elapsedTimei)>0) ? (waitingQueueLengthi*exeTimei-elapsedTimei+exeTimei) : exeTimei;  // exetime method


                if (waitingTimei<shortestWaitingTime){
                    shortestWaitingTime = waitingTimei;
                    index = i;
                }

                // update time
                MyPair<Channel,Long> oldReportTime = component2ServerLastReportUpdateTimes.get(component).get(i);
                MyPair<Channel,Long> newReportTime = new MyPair<Channel,Long>(oldReportTime.left, currentTime);
                component2ServerLastReportUpdateTimes.get(component).set(i,newReportTime);

                // update queue
                MyPair<Channel,Double> oldWaitingQueue = component2ServerWaitingQueues.get(component).get(i);
                //double newQueueLength = ((waitingQueueLengthi-elapsedTimei*throughputi)>0) ? (waitingQueueLengthi-elapsedTimei*throughputi) : 0.0; // throughput + exetime method
                double newQueueLength = ((waitingQueueLengthi - elapsedTimei/exeTimei)>0) ? (waitingQueueLengthi - elapsedTimei/exeTimei) : 0.0;   // exetime method
                MyPair<Channel,Double> newWaitingQueue = new MyPair<Channel,Double>(oldWaitingQueue.left, newQueueLength);
                component2ServerWaitingQueues.get(component).set(i,newWaitingQueue);
            }

            MyPair<Channel,Double> oldWaitingQueue = component2ServerWaitingQueues.get(component).get(index);
            MyPair<Channel,Double> newWaitingQueue = new MyPair<Channel,Double>(oldWaitingQueue.left,oldWaitingQueue.right+1.0);
            component2ServerWaitingQueues.get(component).set(index,newWaitingQueue);

            Channel ch = component2ServerExecutionTimes.get(component).get(index).left;
            return ch;
        }
    }

    /** update the execution time of servers according to the feedback reports **/
    public static void updateComponentServerExeTime(String component, Channel ch, double time) {
        MyPair<Channel, Double> newExeTime = new MyPair(ch, time);
        if(component2ServerExecutionTimes.keySet().contains(component)) {
            int index;
            for (index = 0; index<component2ServerExecutionTimes.get(component).size(); index++){
                if (ch.equals(component2ServerExecutionTimes.get(component).get(index).left))
                    break;
            }
            if (index == component2ServerExecutionTimes.get(component).size()) {      // add execution time for certain address
                component2ServerExecutionTimes.get(component).add(newExeTime);
            } else {       // update execution time for certain address
                component2ServerExecutionTimes.get(component).set(index,newExeTime);
            }
        } else{
            CopyOnWriteArrayList<MyPair<Channel,Double>> ServerExeTimes = new CopyOnWriteArrayList<MyPair<Channel,Double>>();
            ServerExeTimes.add(newExeTime);
            component2ServerExecutionTimes.put(component, ServerExeTimes);
        }
    }

    /** update the throughput of servers according to the feedback reports **/
    public static void updateComponentServerThroughput(String component, Channel ch, double throughput){
        MyPair<Channel, Double> newThroughput = new MyPair(ch, throughput);
        if(component2ServerThroughputs.keySet().contains(component)) {
            int index;
            for (index = 0; index<component2ServerThroughputs.get(component).size(); index++){
                if (ch.equals(component2ServerThroughputs.get(component).get(index).left))
                    break;
            }
            if (index == component2ServerThroughputs.get(component).size()) { // add
                component2ServerThroughputs.get(component).add(newThroughput);
            } else { // update
                component2ServerThroughputs.get(component).set(index,newThroughput);
            }
        } else{
            CopyOnWriteArrayList<MyPair<Channel,Double>> ServerThroughputs = new CopyOnWriteArrayList<MyPair<Channel,Double>>();
            ServerThroughputs.add(newThroughput);
            component2ServerThroughputs.put(component, ServerThroughputs);
        }
    }

    /** update the expect waiting queue of servers according to the feedback reports **/
    public static void updateComponentServerWaitingQueue(String component, Channel ch, double queueLength) {
        MyPair<Channel, Double> newWaitingQueue = new MyPair(ch, queueLength);
        if(component2ServerWaitingQueues.keySet().contains(component)) {
            int index;
            for (index = 0; index<component2ServerWaitingQueues.get(component).size(); index++){
                if (ch.equals(component2ServerWaitingQueues.get(component).get(index).left))
                    break;
            }
            if (index == component2ServerWaitingQueues.get(component).size()) {
                component2ServerWaitingQueues.get(component).add(newWaitingQueue);
            } else {
                component2ServerWaitingQueues.get(component).set(index,newWaitingQueue);
            }
        } else{
            CopyOnWriteArrayList<MyPair<Channel,Double>> ServerWaitingQueues = new CopyOnWriteArrayList<MyPair<Channel,Double>>();
            ServerWaitingQueues.add(newWaitingQueue);
            component2ServerWaitingQueues.put(component, ServerWaitingQueues);
        }
    }

    /** update the last report updating time of servers **/
    public static void updateComponentServerLastReportUpdateTime(String component, Channel ch, Long lastUpdateTime){
        MyPair<Channel, Long> newLastUpdateTime = new MyPair(ch, lastUpdateTime);
        if(component2ServerLastReportUpdateTimes.keySet().contains(component)) {
            int index;
            for (index = 0; index<component2ServerLastReportUpdateTimes.get(component).size(); index++){
                if (ch.equals(component2ServerLastReportUpdateTimes.get(component).get(index).left))
                    break;
            }
            if (index == component2ServerLastReportUpdateTimes.get(component).size()) {
                component2ServerLastReportUpdateTimes.get(component).add(newLastUpdateTime);
            } else {
                component2ServerLastReportUpdateTimes.get(component).set(index, newLastUpdateTime);
            }
        } else{
            CopyOnWriteArrayList<MyPair<Channel,Long>> ServerLastUpdateTimes= new CopyOnWriteArrayList<MyPair<Channel,Long>>();
            ServerLastUpdateTimes.add(newLastUpdateTime);
            component2ServerLastReportUpdateTimes.put(component, ServerLastUpdateTimes);
        }
    }
}
