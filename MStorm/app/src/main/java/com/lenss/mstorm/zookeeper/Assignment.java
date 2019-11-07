package com.lenss.mstorm.zookeeper;

import com.lenss.mstorm.utils.MyPair;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Assignment implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static final int FSTSCHE = 0;
    public static final int FSTRESCHE = 1;
    public static final int RESCHE = 2;

    private int assignId;
    private String serTopology;
    private String sourceAddr;
    private String apk;
    private double assignMetric;
    private int assignType;
    private ArrayList<String> addresses = new ArrayList<String>(); 			// Address of all available nodes
    private ArrayList<String> spoutAddresses=new ArrayList<String>();			// Address of all spout nodes
    private HashMap<Integer,String> task2Node=new HashMap<Integer,String>();		// TaskID ==> Address
    private HashMap<Integer,String> task2Component=new  HashMap<Integer,String>();	// TaskID ==> Component
    private HashMap<String, ArrayList<Integer>> node2Tasks = new HashMap<String, ArrayList<Integer>>();				// Address  ==> List<TaskID>
    private HashMap<String, ArrayList<Integer>> component2Tasks = new HashMap<String, ArrayList<Integer>>();  		// Component ==> List<TaskID>
    private HashMap<String, ArrayList<String>> component2Nodes = new HashMap<String, ArrayList<String>>();			// Component ==> List<Address>
    private HashMap<Integer,MyPair<Integer,Integer>> port2TaskPair = new HashMap<Integer,MyPair<Integer,Integer>>();    // port ==> <TaskID, TaskID>
    private ArrayList<String> assginedNodes = new ArrayList<String>();   // The nodes that are assigned with tasks
    private ArrayList<String> preAssignedNodes = new ArrayList<String>(); // The nodes that are previously assigned with tasks
    private int[][] node2NodeConnection; // Matrix tells how to establish connections among nodes


    public void setAssignId(int id)
    {
        assignId=id;
    }

    public int getAssignId()
    {
        return assignId;
    }

    public void setSerTopology(String serTopology)
    {
        this.serTopology=serTopology;
    }

    public String getSerTopology()
    {
        return serTopology;
    }

    public void setSourceAddr(String addr){
        sourceAddr = addr;
    }

    public String getSourceAddr(){
        return sourceAddr;
    }

    public void setApk(String apk){
        this.apk = apk;
    }

    public String getApk(){
        return apk;
    }

    public void setAssignMetric(double metric){
        assignMetric = metric;
    }

    public double getAssignMetric(){
        return assignMetric;
    }

    public void setAssignType(int type){
        assignType = type;
    }

    public int getAssignType(){
        return assignType;
    }

    public void addAddress(String addr){
        addresses.add(addr);
    }

    public void removeAddress(String addr){
        addresses.remove(addr);
    }

    public ArrayList<String> getAddresses(){
        return addresses;
    }

    public void addSpoutAddr(String addr){
        spoutAddresses.add(addr);
    }

    public void removeSpoutAddr(String addr){
        spoutAddresses.remove(addr);
    }

    public ArrayList<String> getSpoutAddr(){
        return spoutAddresses;
    }

    public HashMap<Integer,String> getTask2Node()
    {
        return task2Node;
    }

    public HashMap<Integer,String> getTask2Component()
    {
        return task2Component;
    }

    public HashMap<String, ArrayList<Integer>> getNode2Tasks(){
        return node2Tasks;
    }

    public HashMap<String, ArrayList<Integer>> getComponent2Tasks(){
        return component2Tasks;
    }

    public HashMap<String, ArrayList<String>> getComponent2Nodes(){
        return component2Nodes;
    }

    public HashMap<Integer,MyPair<Integer,Integer>> getPort2TaskPair(){
        return port2TaskPair;
    }

    public void assgin(String node, int taskid, String component) {
        task2Node.put(taskid,node);
        task2Component.put(taskid, component);

        if(node2Tasks.containsKey(node)){
            node2Tasks.get(node).add(taskid);
        } else {
            ArrayList<Integer> tasks = new ArrayList<Integer>();
            tasks.add(taskid);
            node2Tasks.put(node, tasks);
        }

        if (component2Tasks.containsKey(component)){
            component2Tasks.get(component).add(taskid);
        } else {
            ArrayList<Integer> tasks = new ArrayList<Integer>();
            tasks.add(taskid);
            component2Tasks.put(component, tasks);
        }

        if(component2Nodes.containsKey(component)){
            component2Nodes.get(component).add(node);
        } else {
            ArrayList<String> addresses = new ArrayList<String>();
            addresses.add(node);
            component2Nodes.put(component, addresses);
        }
    }

    public void assignPort(int port, int taskID, int downStreamTaskID){
        MyPair<Integer, Integer> taskPair = new MyPair<Integer,Integer>(taskID,downStreamTaskID);
        port2TaskPair.put(port, taskPair);
    }

    public void addAssginedNodes(String addr){
        if(!assginedNodes.contains(addr))
            assginedNodes.add(addr);
    }

    public ArrayList<String> getAssginedNodes(){
        return assginedNodes;
    }

    public void setPreAssignedNodes(ArrayList<String> preAssNodes){
        preAssignedNodes = preAssNodes;
    }

    public ArrayList<String> getPreAssignedNodes(){
        return preAssignedNodes;
    }

    public void setNode2NodeConnection(int[][] n2nConnection) {
        node2NodeConnection = n2nConnection;
    }

    public int[][] getNode2NodeConnection(){
        return node2NodeConnection;
    }
}