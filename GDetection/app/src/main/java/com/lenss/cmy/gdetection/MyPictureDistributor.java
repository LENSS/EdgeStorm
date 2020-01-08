package com.lenss.cmy.gdetection;

import android.os.Environment;
import android.os.FileObserver;
import android.os.SystemClock;
import android.support.annotation.Nullable;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.communication.internodes.StreamSelector;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.topology.Distributor;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by cmy on 8/14/19.
 */

public class MyPictureDistributor extends Distributor {
    private final String TAG="MyPictureDistributor";
    private final String folderName = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/RawPic/";
    Logger logger;
    FileInputStream fis;
    static FileObserver fileObserver;
    int taskID;

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
        File rawPicFolder = new File(folderName);
        rawPicFolder.mkdir();
        taskID = getTaskID();
    }

    @Override
    public void execute() {
        fileObserver = new FileObserver(folderName) {
            @Override
            public void onEvent(int event, @Nullable String file) {
                if(file== null || file.equals(".probe"))
                    return;
                if(event == FileObserver.CLOSE_WRITE){
                    readStream(file);
                }
            }

            public void readStream(String file){
                try {
                    long enterTime = SystemClock.elapsedRealtimeNanos();
                    fis = new FileInputStream(folderName+file);
                    int lengthOfFrame = fis.available();
                    if (lengthOfFrame > 0) {
                        byte[] frame = new byte[lengthOfFrame];
                        fis.read(frame);

                        InternodePacket pktSend = new InternodePacket();
                        pktSend.ID = enterTime;
                        pktSend.type = InternodePacket.TYPE_DATA;
                        pktSend.fromTask = getTaskID();
                        pktSend.complexContent = frame;
                        pktSend.traceTask.add("MPD_"+getTaskID());
                        pktSend.traceTaskEnterTime.put("MPD_" + getTaskID(), enterTime);
                        long exitTime = SystemClock.elapsedRealtimeNanos();
                        pktSend.traceTaskExitTime.put("MPD_" + getTaskID(), exitTime);

                        // Pkt of the first component is not received from others.
                        // Therefore, it needs to run StreamSelector.select() by itself
                        if(StreamSelector.select(taskID)==StreamSelector.KEEP) {
                            // add entry and begin processing time for first task, because it does not
                            // call MessageQueue.collect() and MessageQueue.retrieveIncomingQueue()
                            StatusOfLocalTasks.task2EntryTimesForFstComp.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2EntryTimes.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2EntryTimesUpStream.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2EntryTimesNimbus.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).add(enterTime);

                            String component = MyFaceDetector.class.getName();
                            try {
                                MessageQueues.emit(pktSend, taskID, component);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        // performance log
                        String report = "SEND:" + "ID:" + pktSend.ID + "\n";
                        try {
                            FileWriter fw = new FileWriter(ComputingNode.EXEREC_ADDRESSES, true);
                            fw.write(report);
                            fw.close();
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        //logger.info("TIME STAMP, PULL A PICTURE INTO MSTORM " + file);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if(fis!=null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        fileObserver.startWatching();
    }

    @Override
    public void postExecute() {
        if(fis!=null) {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
