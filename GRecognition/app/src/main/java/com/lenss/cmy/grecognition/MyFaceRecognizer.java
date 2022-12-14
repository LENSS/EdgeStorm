package com.lenss.cmy.grecognition;


import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Environment;
import android.os.SystemClock;

import com.google.gson.annotations.Expose;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;
import com.tzutalin.dlib.Constants;
import com.tzutalin.dlib.FaceRec;
import com.tzutalin.dlib.VisionDetRet;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * Created by cmy on 7/26/19.
 */

public class MyFaceRecognizer extends Processor {
    private final String TAG="MyFaceRecognizer";
    Logger logger;
    private FaceRec mFaceRec;

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
    }

    @Override
    public void execute(){
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
            if(pktRecv != null){
                long enterTime = SystemClock.elapsedRealtimeNanos();
                byte[] frame = pktRecv.complexContent;
                //logger.info("TIME STAMP 8, FACE RECOGNIZER RECEIVES A FRAME, "+ taskID);
                BitmapFactory.Options bitmapFatoryOptions = new BitmapFactory.Options();
                Bitmap bitmap = BitmapFactory.decodeByteArray(frame,0,frame.length,bitmapFatoryOptions);

                mFaceRec = new FaceRec(Constants.getDLibDirectoryPath());

                List<VisionDetRet> dfaces = mFaceRec.recognize(bitmap);

                if(dfaces.size()>1){
                    logger.info("why has multiple faces ?");
                }

                for (VisionDetRet vdr: dfaces) {
                    Bitmap bmface;
                    try {
                        bmface = Bitmap.createBitmap(bitmap, vdr.getLeft(), vdr.getTop(), vdr.getRight() - vdr.getLeft(), vdr.getBottom() - vdr.getTop());
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }

                    if (bmface != null) {
                        ByteArrayOutputStream stream = new ByteArrayOutputStream();
                        bmface.compress(Bitmap.CompressFormat.JPEG, 100, stream);
                        byte[] imageByteArray = stream.toByteArray();
                        InternodePacket pktSend = new InternodePacket();
                        pktSend.ID = pktRecv.ID;
                        pktSend.type = InternodePacket.TYPE_DATA;
                        pktSend.fromTask = taskID;
                        pktSend.simpleContent.put("name",vdr.getLabel().split("\\.")[0]);
                        pktSend.complexContent = imageByteArray;
                        if(pktSend.traceTask.contains("MFR_"+taskID)){
                            logger.info("Why here has 2 ?");
                            continue;
                        }
                        pktSend.traceTask = pktRecv.traceTask;
                        pktSend.traceTask.add("MFR_"+taskID);
                        pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                        pktSend.traceTaskEnterTime.put("MFR_"+taskID, enterTime);
                        pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                        long exitTime = SystemClock.elapsedRealtimeNanos();
                        pktSend.traceTaskExitTime.put("MFR_"+taskID, exitTime);
                        //logger.info("TIME STAMP 9, FACE RECOGNIZER SAVES A FACE, "+ taskID);
                        String component = MyFaceSaver.class.getName();
                        try {
                            MessageQueues.emit(pktSend, taskID, component);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    @Override
    public void postExecute(){

    }
}
