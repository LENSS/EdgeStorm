package com.android.grecognition;

import android.os.Environment;
import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by cmy on 8/14/19.
 */

public class MyFaceSaver extends Processor {
    @Expose
    private final String TAG="MyFaceSaver";
    private String PIC_URL = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/StreamFDPic/";
    Logger logger;

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
    }

    @Override
    public void execute() {
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            if(pktRecv!=null) {
                byte[] frame = pktRecv.complexContent;
                String name = pktRecv.simpleContent.get("name");
                saveFaceFileWithName(frame,name);
                logger.info("TIME STAMP, SAVES RECOGNIZED FACES INTO FILE SYSTEM, " + System.nanoTime());
            }
        }
    }

    @Override
    public void postExecute() {}

    public void saveFaceFileWithName(byte[] jpgBytes, String name){
        try {
            File file = new File(PIC_URL + System.nanoTime() + "_" + name);
            FileOutputStream fOut = new FileOutputStream(file);
            fOut.write(jpgBytes);
            fOut.flush();
            fOut.close();
        } catch (IOException e) {
            logger.error("file not found for stream fd pictures");
        }
    }
}
