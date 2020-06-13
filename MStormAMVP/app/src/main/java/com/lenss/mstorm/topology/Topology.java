package com.lenss.mstorm.topology;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.utils.Serialization;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Topology: used as API for user to submit their application to mStorm
 */

public class Topology implements Serializable {

    private static final long serialVersionUID = 4048517610525361038L;

    // schedule requirements
    public static final int Schedule_Local = 1;
    public static final int Schedule_Any =  2;

    // stream grouping choice
    public static final int Shuffle = 0;
    public static final int MinSojournTime = 1;
    public static final int SojournTimeProb = 2;
    public static final int MinEWT = 3;

    @Expose
    private int componentNum;       // num of component
    @Expose
    private int distributorNum;    // num of distributor
    @Expose
    private String[] components;    // component names
    @Expose
    private int[][] graph;          // component relationship graph
    @Expose
    private HashMap<String,Integer> parallelismHints;      // parallelism of each component
    @Expose
    private HashMap<String,String> serInstances;    // serialized instance for each component
    @Expose
    private HashMap<String,Integer> groupings;      // stream grouping method of each component
    @Expose
    private HashMap<String, Integer> scheduleRequirements;      // schedule requirement of each component
    @Expose
    private int srcSpeed; // The avg input rate

    private int compIndex;

    public Topology(int compNum) {
        componentNum = compNum;
        distributorNum = 0;
        components = new String[componentNum];
        graph = new int[componentNum][componentNum];
        parallelismHints=new HashMap<String,Integer>();
        groupings=new HashMap<String,Integer>();
        serInstances=new HashMap<String,String>();
        scheduleRequirements = new HashMap<String,Integer>();
        compIndex = 0;
    }

    public void setDistributor(Object distributor,int parall, int schReq) {
        if(compIndex<componentNum) {
            String distributorName = distributor.getClass().getName();
            components[compIndex++] = distributorName;
            distributorNum++;
            parallelismHints.put(distributorName, parall);
            serInstances.put(distributorName, Serialization.Serialize(distributor));
            scheduleRequirements.put(distributorName, schReq);
        }
        else{
            throw new IndexOutOfBoundsException("Index " + compIndex + " is out of bounds!");
        }
    }

    public void setProcessor(Object processor, int parall, int grouping, int schReq){
        if(compIndex<componentNum) {
            String processorName = processor.getClass().getName();
            components[compIndex++] = processorName;
            parallelismHints.put(processorName, parall);
            serInstances.put(processorName, Serialization.Serialize(processor));
            groupings.put(processorName, grouping);
            scheduleRequirements.put(processorName, schReq);
        }
        else{
            throw new IndexOutOfBoundsException("Index " + compIndex + " is out of bounds!");
        }
    }

    public void setDownStreamComponents(Object component, ArrayList<Object> downStreamComponents){
        int index = compToIndex(component.getClass().getName());
        ArrayList<Integer> downStreamCompIndexes = compsToIndexes(downStreamComponents);
        for (int i =0;i<downStreamCompIndexes.size();i++)
            graph[index][downStreamCompIndexes.get(i)] = 1;
    }

    public ArrayList<String> getDownStreamComponents(String componentName){
        int index = compToIndex(componentName);
        ArrayList<String> downStreamComponents = new ArrayList<String>();
        for (int i =0;i<componentNum;i++) {
            if (graph[index][i] == 1){
                downStreamComponents.add(components[i]);
            }
        }
        return downStreamComponents;
    }

    public ArrayList<String> getUpStreamComponents(String componentName){
        int index = compToIndex(componentName);
        ArrayList<String> upStreamComponents = new ArrayList<String>();
        for (int i =0;i<componentNum;i++) {
            if (graph[i][index] == 1){
                upStreamComponents.add(components[i]);
            }
        }
        return upStreamComponents;
    }

    public int getComponentNum(){
        return componentNum;
    }

    public int getDistributorNum() {
        return distributorNum;
    }

    public String[] getComponents(){
        return components;
    }

    public int[][] getGraph(){
        return graph;
    }

    public HashMap<String,Integer> getParallelismHints()
    {
        return parallelismHints;
    }

    public void setParallelismHints(String compName, int parall){
        parallelismHints.put(compName, parall);
    }

    public HashMap<String,Integer> getGroupings(){return groupings;}

    public HashMap<String, String> getSerInstances(){return serInstances;}

    public HashMap<String, Integer> getScheduleRequirements(){
        return scheduleRequirements;
    }

    public ArrayList<Integer> compsToIndexes(ArrayList<Object> comps){
        ArrayList<Integer> indexes = new ArrayList<Integer>();
        for (int i=0; i< comps.size();i++){
            int index = compToIndex(comps.get(i).getClass().getName());
            if(index != -1)
                indexes.add(index);
        }
        return indexes;
    }

    public int compToIndex(String comp){
        for(int i = 0; i< componentNum; i++){
            if (components[i].equals(comp))
                return i;
        }
        return -1;
    }

    public int getSrcSpeed(){
        return srcSpeed;
    }

    public void setSrcSpeed(int speed){
        srcSpeed = speed;
    }
}
