package com.lenss.liuyi.gassistant;

import android.os.Environment;
import android.os.SystemClock;

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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class MyVoiceSaver extends Processor {

    private final String TAG = "MyVoiceSaver";

    private String VOICE_TEXT_PATH = Environment.getExternalStorageDirectory().getPath() + "distressnet/MStorm/VoiceText/";
    private String MDFS_VOICE_TEXT_PATH = Environment.getExternalStorageDirectory().getPath() + "distressnet/MStorm/MDFSVoiceText";


    Logger logger;
    SimpleDateFormat formatter;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);
        formatter = new SimpleDateFormat("yyyyMMdd_HH:mm:ss.SSS");
    }

    @Override
    public void execute(){
        int taskID = getTaskID();
        while(!Thread.currentThread().interrupted()){
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
            if(pktRecv != null){
                logger.debug("Pkt received at MyVoiceSaver!");
                long enterTime = SystemClock.elapsedRealtimeNanos();
                byte[] textResultByteArray = pktRecv.complexContent;
                saveTextFileWithName(textResultByteArray);
                long exitTime = SystemClock.elapsedRealtimeNanos();

                if(StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).size() > 0){
                    long startProcessingTime = StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).remove(0);
                    long processingTime = exitTime - startProcessingTime;
                    StatusOfLocalTasks.task2ProcessingTimesUpStream.get(taskID).add(processingTime);
                }

                StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(exitTime);
                StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(exitTime);

                if(StatusOfLocalTasks.task2EntryTimes.get(taskID).size() > 0){
                    long entryTime = StatusOfLocalTasks.task2EntryTimes.get(taskID).remove(0);
                    long responseTime = exitTime - entryTime;
                    StatusOfLocalTasks.task2EmitTimesUpStream.get(taskID).add(responseTime);
                    StatusOfLocalTasks.task2EmitTimesNimbus.get(taskID).add(responseTime);
                }

                // performance log
                String report = "RECV:" + "ID:" +pktRecv.ID + "--";
                for(String task : pktRecv.traceTask){
                    report += task + ":" + "(" + pktRecv.traceTaskEnterTime.get(task) + "," + pktRecv.traceTaskExitTime.get(task) + ")" + ",";
                    report += "MVS_" + getTaskID() + ":" + "(" + enterTime + "," + exitTime + ")" + "," + "ResponseTime:" + (exitTime-pktRecv.ID)/1000000.0 + "\n";
                }

                try{
                    FileWriter fw = new FileWriter(ComputingNode.EXEREC_ADDRESSES, true);
                    fw.write(report);
                    fw.close();
                }catch (FileNotFoundException e){
                    e.printStackTrace();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void postExecute(){

    }

    public void saveTextFileWithName(byte[] str){
        try{
            String fileName = VOICE_TEXT_PATH + formatter.format(Calendar.getInstance().getTimeInMillis());
            FileOutputStream fOut = new FileOutputStream(fileName);
            fOut.write(str);
            fOut.flush();
            fOut.close();

            //store to MDFS
            MDFSClient.put(fileName, MDFS_VOICE_TEXT_PATH);
        } catch (IOException e){
            logger.error("Assistant Text File not saved successfully!");
        }
    }

}
