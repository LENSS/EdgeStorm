package com.lenss.liuyi;

import android.os.Environment;
import android.os.FileObserver;
import android.os.SystemClock;

import androidx.annotation.Nullable;

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

//import com.lenss.mstorm.topology.Processor;

public class MyVoiceDistributor extends Distributor {

    private final String TAG = "MyVoiceDistributor";
    private final String folderName = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/RawVoice/";
    Logger logger;
    FileInputStream fis;
    private static FileObserver fileObserver;
    int taskID;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);
        File rawVoiceFolder = new File(folderName);
        rawVoiceFolder.mkdir();
        taskID = getTaskID();
    }

    @Override
    public void execute(){
        fileObserver = new FileObserver(folderName) {
            @Override
            public void onEvent(int event, @Nullable String file) {
                if(file == null || file.equals(".probe")) return;
                if(file.substring(file.lastIndexOf('.')).equals(".wav")) return;
                logger.info("Detected new pcm file: " + file);
                if(event == FileObserver.CLOSE_WRITE){
                    readStream(file);
                }
            }

            public void readStream(String file){
                try{
                    long enterTime = SystemClock.elapsedRealtimeNanos();
                    logger.info(" Distributor start to read a new FileInput Stream : " + file);
                    fis = new FileInputStream(folderName + file);
                    int lengthOfVoiceBytes = fis.available();
                    if(lengthOfVoiceBytes > 0){
                        byte[] voiceByteArray = new byte[lengthOfVoiceBytes];
                        fis.read(voiceByteArray);
                        logger.info(String.format("distributor has load %d bytes from %s", lengthOfVoiceBytes, file));
                        InternodePacket pktSend = new InternodePacket();
                        pktSend.ID = enterTime;
                        pktSend.type = InternodePacket.TYPE_DATA;
                        pktSend.fromTask = getTaskID();
                        pktSend.complexContent = voiceByteArray;
                        pktSend.traceTask.add("MVP_" + getTaskID());
                        pktSend.traceTaskEnterTime.put("MVP_" + getTaskID(), enterTime);
                        long exitTime = SystemClock.elapsedRealtimeNanos();
                        pktSend.traceTaskExitTime.put("MVP_" + getTaskID(), exitTime);

                        if(StreamSelector.select(taskID) == StreamSelector.KEEP){
                            StatusOfLocalTasks.task2EntryTimesForFstComp.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2EntryTimes.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2EntryTimesUpStream.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2EntryTimesNimbus.get(taskID).add(enterTime);
                            StatusOfLocalTasks.task2BeginProcessingTimes.get(taskID).add(enterTime);
                            String component = MyVoiceConverter.class.getName();
                            try{
                                MessageQueues.emit(pktSend, taskID, component);
                            } catch (InterruptedException e){
                                e.printStackTrace();
                            }
                        }

                        String report = "SEND:" + "ID:" +  pktSend.ID + "\n";
                        logger.info(report);
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
                } catch (IOException e){
                    e.printStackTrace();
                }
                if( fis != null){
                    try{
                        fis.close();
                    }catch (IOException e){
                        e.printStackTrace();
                    }

                }
            }
        };
//        logger.info(TAG +  "This is before the file observer watch");
        fileObserver.startWatching();
//        logger.info(TAG +  "This is after the file observer watch");
    }

    @Override
    public void postExecute(){
        if(fis != null){
            try{
                fis.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

}
