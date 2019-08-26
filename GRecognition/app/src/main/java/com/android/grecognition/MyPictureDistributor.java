package com.android.grecognition;

import android.os.Environment;
import android.os.FileObserver;
import android.support.annotation.Nullable;
import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Distributor;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by cmy on 8/14/19.
 */

public class MyPictureDistributor extends Distributor {
    @Expose
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
                    fis = new FileInputStream(folderName+file);
                    int lengthOfFrame = fis.available();
                    if (lengthOfFrame > 0) {
                        byte[] frame = new byte[lengthOfFrame];
                        fis.read(frame);
                        String component = MyFaceDetector.class.getName();
                        try {
                            InternodePacket pktSend = new InternodePacket();
                            pktSend.type = InternodePacket.TYPE_DATA;
                            pktSend.fromTask = getTaskID();
                            pktSend.complexContent = frame;
                            pktSend.ID = System.nanoTime();
                            MessageQueues.emit(pktSend, getTaskID(), component);
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
