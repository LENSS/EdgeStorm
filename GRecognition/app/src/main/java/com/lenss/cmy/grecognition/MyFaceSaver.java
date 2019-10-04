package com.lenss.cmy.grecognition;

import android.os.Environment;
import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.topology.Processor;
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
                long enterTime = System.nanoTime();
                byte[] frame = pktRecv.complexContent;
                String name = pktRecv.simpleContent.get("name");
                saveFaceFileWithName(frame,name);
                logger.info("TIME STAMP, SAVES RECOGNIZED FACES INTO FILE SYSTEM, " + System.nanoTime());
                long exitTime = System.nanoTime();

                // performance log
                String report = "RECV:" + "ID:" +pktRecv.ID + "--";
                for(String task: pktRecv.traceTask){
                    report += task + ":" + "(" + pktRecv.traceTaskEnterTime.get(task) + "," + pktRecv.traceTaskExitTime.get(task) + ")" + "--";
                }
                report += "MFS_" + getTaskID() + ":" + "(" + enterTime + ","  + exitTime + ")" + "\n";
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
            File file = new File(PIC_URL + formatter.format(Calendar.getInstance().getTimeInMillis()) + "_" + name + ".jpg");
            FileOutputStream fOut = new FileOutputStream(file);
            fOut.write(jpgBytes);
            fOut.flush();
            fOut.close();
        } catch (IOException e) {
            logger.error("file not found for stream fd pictures");
        }
    }
}
