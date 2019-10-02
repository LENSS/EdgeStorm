package com.lenss.cmy.gdetection;

import android.os.Environment;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by cmy on 8/14/19.
 */

public class MyFaceSaver extends Processor {
    private final String TAG="MyFaceSaver";
    private String PIC_URL = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/StreamFDPic/";
    Logger logger;
    SimpleDateFormat formatter;

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
        formatter = new SimpleDateFormat("yyyyMMdd_HH:mm:ss.SSS");
    }

    @Override
    public void execute() {
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            if(pktRecv!=null) {
                byte[] frame = pktRecv.complexContent;
                saveFaceFileWithName(frame);
                logger.info("TIME STAMP, SAVES RECOGNIZED FACES INTO FILE SYSTEM, " + System.nanoTime());
            }
        }
    }

    @Override
    public void postExecute() {}

    public void saveFaceFileWithName(byte[] jpgBytes){
        try {
            File file = new File(PIC_URL + formatter.format(Calendar.getInstance().getTimeInMillis()) + ".jpg");
            FileOutputStream fOut = new FileOutputStream(file);
            fOut.write(jpgBytes);
            fOut.flush();
            fOut.close();
        } catch (IOException e) {
            logger.error("file not found for stream fd pictures");
        }
    }
}
