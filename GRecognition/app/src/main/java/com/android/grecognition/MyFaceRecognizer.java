package com.android.grecognition;


import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Environment;

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
    @Expose
    private final String TAG="MyFaceRecognizer";
    private String PIC_URL = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/StreamFDPic/";
    Logger logger;
    private FaceRec mFaceRec;

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
    }

    @Override
    public void execute(){
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            if(pktRecv != null){
                byte[] frame = pktRecv.complexContent;
                logger.info("TIME STAMP 8, FACE RECOGNIZER RECEIVES A FRAME, "+ getTaskID());
                BitmapFactory.Options bitmapFatoryOptions = new BitmapFactory.Options();
                Bitmap bitmap = BitmapFactory.decodeByteArray(frame,0,frame.length,bitmapFatoryOptions);

                mFaceRec = new FaceRec(Constants.getDLibDirectoryPath());
                List<VisionDetRet> dfaces = mFaceRec.recognize(bitmap);

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
                        pktSend.type = InternodePacket.TYPE_DATA;
                        pktSend.fromTask = getTaskID();
                        pktSend.simpleContent.put("name",vdr.getLabel());
                        pktSend.complexContent = imageByteArray;
                        logger.info("TIME STAMP 9, FACE RECOGNIZER SAVES A FACE, "+ getTaskID());
                        String component = MyFaceSaver.class.getName();
                        try {
                            MessageQueues.emit(pktSend, getTaskID(), component);
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
