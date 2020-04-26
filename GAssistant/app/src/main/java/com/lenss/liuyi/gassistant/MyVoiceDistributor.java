package com.lenss.liuyi.gassistant;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.communication.internodes.StreamSelector;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.status.StatusOfLocalTasks;
import com.lenss.mstorm.topology.Distributor;
//import com.lenss.mstorm.topology.Processor;

import android.media.AudioFormat;
import android.media.AudioRecord;
import android.media.MediaRecorder.AudioSource;
import android.os.Environment;
import android.os.FileObserver;
import android.os.SystemClock;

import androidx.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.log4j.Logger;

public class MyVoiceDistributor extends Distributor {

    private final String TAG = "MyVoiceDistributor";
    private final String folderName = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/RawVoice/";
    Logger logger;
    FileInputStream fis;
    static FileObserver fileObserver;
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
            public void onEvent(int event, @Nullable String path) {
                if(path == null || path.equals(".probe")) return;
                if(event == FileObserver.CLOSE_WRITE){
                    readStream(path);
                }
            }

            public void readStream(String file){
                try{
                    long enterTime = SystemClock.elapsedRealtimeNanos();
                    logger.debug(TAG +  " Distributor start to new a FileInput Stream");
                    fis = new FileInputStream(folderName + file);
                    int lengthOfVoiceBytes = fis.available();
                    if(lengthOfVoiceBytes > 0){
                        byte[] voiceByteArray = new byte[lengthOfVoiceBytes];
                        fis.read(voiceByteArray);

                        InternodePacket pktSend = new InternodePacket();
                        pktSend.ID = enterTime;
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

        fileObserver.startWatching();
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
