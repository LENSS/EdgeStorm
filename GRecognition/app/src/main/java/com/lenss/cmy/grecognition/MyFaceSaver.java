package com.lenss.cmy.grecognition;

import android.os.Environment;
import android.os.SystemClock;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.topology.Processor;
import com.lenss.mstorm.utils.MDFSClient;

import org.apache.log4j.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by cmy on 8/14/19.
 */

public class MyFaceSaver extends Processor {
    private final String TAG="MyFaceSaver";
    private String PIC_URL = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/StreamFDPic/";
    private String MDFS_PIC_URL = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/MDFSPic/";

    Logger logger;
    SimpleDateFormat formatter;

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
        formatter = new SimpleDateFormat("yyyyMMdd_HH:mm:ss.SSS");

    }

    @Override
    public void execute() {
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
            if(pktRecv!=null) {
                long enterTime = SystemClock.elapsedRealtimeNanos();
                byte[] frame = pktRecv.complexContent;
                String name = pktRecv.simpleContent.get("name");
                saveFaceFileWithName(frame,name);
                long exitTime = SystemClock.elapsedRealtimeNanos();
                //logger.info("TIME STAMP, SAVES RECOGNIZED FACES INTO FILE SYSTEM, " + exitTime);

                // calculate processing and response time for the last task, because it does not
                // call MessageQueue.emit() and MessageQueue.retrieveOutgoingQueue() in Dispatcher class
                if(StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).size()>0) {
                    long startProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).remove(0);
                    long processingTime = exitTime - startProcessingTime;
                    StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
                }

                StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(exitTime);
                StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(exitTime);

                if(StatusOfLocalTasks.task2EntryTimes.get(taskID).size()>0) {
                    long entryTime = StatusOfLocalTasks.task2EntryTimes.get(taskID).remove(0);
                    long responseTime = exitTime - entryTime;
                    StatusOfLocalTasks.task2ResponseTimesUpStream.get(taskID).add(responseTime);
                    StatusOfLocalTasks.task2ResponseTimesNimbus.get(taskID).add(responseTime);
                }

                // performance log
                String report = "SEND:" + pktRecv.ID + ",";
                for(String task: pktRecv.traceTask){
                    report += task + ":" + "(" + pktRecv.traceTaskEnterTime.get(task) + "," + pktRecv.traceTaskExitTime.get(task) + ")" + ",";
                }
                report += "MFS_" + taskID + ":" + "(" + enterTime + ","  + exitTime + ")" + ","
                        + "ResponseTime:" + (exitTime-pktRecv.ID)/1000000.0 + "\n";
                try {
                    FileWriter fw = new FileWriter(ComputingNode.EXEREC_ADDRESSES, true);
                    fw.write(report);
                    fw.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void postExecute() {}

    public void saveFaceFileWithName(byte[] jpgBytes, String name){
        try {
            // store to localFile System
            String fileName = PIC_URL + formatter.format(Calendar.getInstance().getTimeInMillis()) + "_" + name + ".jpg";
            File file = new File(fileName);
            FileOutputStream fOut = new FileOutputStream(file);
            fOut.write(jpgBytes);
            fOut.flush();
            fOut.close();

            // store to MDFS
            MDFSClient.put(fileName, MDFS_PIC_URL);
        } catch (IOException e) {
            logger.error("file not found for stream fd pictures");
        }
    }
}
