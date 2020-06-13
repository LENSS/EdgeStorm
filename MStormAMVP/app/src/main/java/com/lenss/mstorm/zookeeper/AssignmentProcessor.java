package com.lenss.mstorm.zookeeper;

/**
 * Created by cmy on 8/8/19.
 */

public interface AssignmentProcessor {

    void listenOnTaskAssignment(String cluster_id);

    void startComputing(String newAssignment);

    void stopComputing();

    void stopComputing(String assignment);

}
