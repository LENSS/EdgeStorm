package com.lenss.cmy.grecognition;

import android.os.Environment;
import android.os.FileObserver;
import android.support.annotation.Nullable;
import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;
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

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
        File rawPicFolder = new File(folderName);
        rawPicFolder.mkdir();
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
                    long enterTime = System.nanoTime();
                    fis = new FileInputStream(folderName+file);
                    int lengthOfFrame = fis.available();
                    if (lengthOfFrame > 0) {
                        byte[] frame = new byte[lengthOfFrame];
                        fis.read(frame);
                        String component = MyFaceDetector.class.getName();
                        try {
                            InternodePacket pktSend = new InternodePacket();
                            pktSend.ID = enterTime;
                            pktSend.type = InternodePacket.TYPE_DATA;
                            pktSend.fromTask = getTaskID();
                            pktSend.complexContent = frame;
                            pktSend.traceTask.add("MPD_"+getTaskID());
                            pktSend.traceTaskEnterTime.put("MPD_" + getTaskID(), enterTime);
                            long exitTime = System.nanoTime();
                            pktSend.traceTaskExitTime.put("MPD_" + getTaskID(), exitTime);
                            MessageQueues.emit(pktSend, getTaskID(), component);

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
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        logger.info("TIME STAMP, PULL A PICTURE INTO MSTORM " + file);
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
