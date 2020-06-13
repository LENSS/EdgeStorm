package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.utils.Helper;

import org.apache.log4j.Logger;

public class StreamSelector {
    private static final String TAG="StreamSelector";
    private static Logger logger = Logger.getLogger(TAG);

    public static final int NO_SELECTION = 0;
    public static final int QUEUE_THRESHOLD_BASED = 1;
    public static final int QUEUE_PROBABILITY_BASED = 2;
    public static final int IO_SPEED_BASED = 3;
    public static final int IO_SPEED_PROBABILITY_BASED = 4;

    public static int MAX_QUEUE_LENGTH = 10;
    public static double DROPPING_THRESHOLD = 0.5;
    public static int SELECT_STRATEGY = NO_SELECTION;

    public static int DROP = 0;
    public static int KEEP = 1;


    public static void setMaxQueueLength(int length){
        MAX_QUEUE_LENGTH = length;
    }

    public static void setStartDroppingThreshold(double threshold){
        DROPPING_THRESHOLD = threshold;
    }

    public static void setSelectStrategy(int strategy){
        SELECT_STRATEGY = strategy;
    }

    public static int select(int taskID){
        int keepORDrop;
        switch(SELECT_STRATEGY){
            case QUEUE_THRESHOLD_BASED:
                keepORDrop = queueThresholdBasedSelect(taskID);
                break;
            case QUEUE_PROBABILITY_BASED:
                keepORDrop = queueProbabilityBasedSelect(taskID);
                break;
            case IO_SPEED_BASED:
                keepORDrop = iOSpeedBasedSelect(taskID);
                break;
            case IO_SPEED_PROBABILITY_BASED:
                keepORDrop = iOSpeedProbabilityBasedSelect(taskID);
                break;
            default:    // no selection
                keepORDrop = KEEP;
                break;
        }
        return keepORDrop;
    }

    public static int queueThresholdBasedSelect(int taskID){
        int currentInQueueLength = MessageQueues.incomingQueues.get(new Integer(taskID)).size();
        int currentOutQueueLength = MessageQueues.outgoingQueues.get(new Integer(taskID)).size();
        logger.debug("InQueue Length: " + currentInQueueLength + "," + "OutQueue Length: " + currentOutQueueLength + "," + "TaskID: " + taskID);
        if(currentInQueueLength < MAX_QUEUE_LENGTH * DROPPING_THRESHOLD && currentOutQueueLength < MAX_QUEUE_LENGTH * DROPPING_THRESHOLD){
            return KEEP;
        } else {
            logger.info("queueThresholdBasedSelect: Stream tuple is dropped at task" + taskID);
            return DROP;
        }
    }

    public static int queueProbabilityBasedSelect(int taskID){
        double dropProbInQueue = 1.0 * MessageQueues.incomingQueues.get(new Integer(taskID)).size()/MAX_QUEUE_LENGTH;
        double dropProbOutQueue = 1.0 * MessageQueues.outgoingQueues.get(new Integer(taskID)).size()/MAX_QUEUE_LENGTH;
        dropProbInQueue = (dropProbInQueue <= 1) ? dropProbInQueue : 1;
        dropProbOutQueue = (dropProbOutQueue <= 1) ? dropProbOutQueue : 1;
        logger.debug("DropProbInQueue: " + dropProbInQueue + "," + "DropProbOutQueue: " + dropProbOutQueue + "," + "TaskID: " + taskID);
        double random = Helper.randDouble(0,1);
        if (random > dropProbInQueue && random > dropProbOutQueue){
            return KEEP;
        } else {
            logger.info("queueProbabilityBasedSelect: Stream tuple is dropped at task" + taskID);
            return DROP;
        }
    }

    public static int iOSpeedBasedSelect(int taskID){
        double inputSpeed = StatusOfLocalTasks.task2InputRate.get(taskID);
        double outputSpeed = StatusOfLocalTasks.task2OutputRate.get(taskID);
        logger.debug("Input Speed: " + inputSpeed + "," + "Output Speed: " + outputSpeed + "," + "TaskID: " + taskID);
        if (inputSpeed <= outputSpeed){
            return KEEP;
        } else {
            logger.info("iOSpeedBasedSelect: Stream tuple is dropped at task" + taskID);
            return DROP;
        }
    }

    public static int iOSpeedProbabilityBasedSelect(int taskID){
        double inputSpeed = StatusOfLocalTasks.task2InputRate.get(taskID);
        double outputSpeed = StatusOfLocalTasks.task2OutputRate.get(taskID);
        double dropProb = (inputSpeed - outputSpeed)/inputSpeed;
        dropProb = (dropProb > 0) ? dropProb : 0;
        logger.debug("Input Speed: " + inputSpeed + "," + "Output Speed: " + outputSpeed + "," + "DropProb: " + dropProb + "," + "TaskID: " + taskID);
        if (Helper.randDouble(0,1) > dropProb){
            return KEEP;
        } else {
            logger.info("iOSpeedBasedSelect: Stream tuple is dropped at task" + taskID);
            return DROP;
        }
    }
}
