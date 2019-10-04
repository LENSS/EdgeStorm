package com.lenss.cmy.grecognition;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Rect;
import android.util.SparseArray;

import com.google.android.gms.vision.Frame;
import com.google.android.gms.vision.face.Face;
import com.google.android.gms.vision.face.FaceDetector;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.topology.Processor;
import com.qualcomm.snapdragon.sdk.face.FaceData;
import com.qualcomm.snapdragon.sdk.face.FacialProcessing;
import com.tzutalin.dlib.FaceDet;
import com.tzutalin.dlib.VisionDetRet;
import org.apache.log4j.Logger;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class MyFaceDetector extends Processor {
    private final String TAG="MyFaceDetector";
    // Qualcomm Face Detection Lib
    private final int QFD = 1;
    boolean isSupported;
    public final int confidence_value = 90;
    private FacialProcessing qFaceDet=null;

    // Dlib Face Detection Lib
    private final int DFD = 2;
    private FaceDet dFaceDet = null;

    // Android Media Face Detection Lib
    private final int AFD = 3;
    private FaceDetector aFaceDet = null;

    private int choosenFD = 0;

    public boolean activityStartedOnce = false;

    Logger logger;

    private void setupFaceDetectionLib(int faceDectLib) {
        if (!activityStartedOnce) { // Check to make sure FacialProcessing object is not created multiple times.
            activityStartedOnce = true;
            if(faceDectLib == QFD) {
                // Check if Facial Recognition feature is supported in the device
                isSupported = FacialProcessing.isFeatureSupported(FacialProcessing.FEATURE_LIST.FEATURE_FACIAL_RECOGNITION);
                if (isSupported) {
                    logger.info("Using Qualcomm Face Detection Library!");
                    qFaceDet = FacialProcessing.getInstance();
                    if (qFaceDet != null) {
                        qFaceDet.setProcessingMode(FacialProcessing.FP_MODES.FP_MODE_VIDEO);
                    } else{
                        logger.error("Cannot get FacialProcessing instance!");
                    }
                    choosenFD = QFD;
                } else {
                    logger.info("The Hardware Does Not Support Qualcomm Face Detection Library, Using Dlib Face Detection Library Instead!");
                    dFaceDet = new FaceDet();
                    choosenFD = DFD;
                }
            } else if(faceDectLib == DFD){
                logger.info("Using Dlib Face Detection Library!");
                dFaceDet = new FaceDet();
                choosenFD = DFD;
            } else if(faceDectLib == AFD) {
                logger.info("Using Android Media Face Detection Library!");
                aFaceDet = new FaceDetector.Builder(MStorm.getContext()).setMode(FaceDetector.ACCURATE_MODE).build();
                choosenFD = AFD;
            } else {
                logger.error("No Such Face Detection Library!");
            }
        }
    }

    @Override
    public void prepare() {
        logger = Logger.getLogger(TAG);
        setupFaceDetectionLib(AFD);
    }

    @Override
    public void execute() {
        // Some controllers to control the processing frequency
        int PicProcessController = 0;
        final int PROCESS_FREQ = 1;

        while(!Thread.currentThread().interrupted())
        {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            // Logical code starts
            if(pktRecv!=null){
                long enterTime = System.nanoTime();
                byte[] frame = pktRecv.complexContent;
                logger.info("TIME STAMP 5, FACE DETECTOR RECEIVES A FRAME, "+ getTaskID());
                FaceData[] qfaces = null;           // QFD
                List<VisionDetRet> dfaces = null;   // For DFD
                SparseArray<Face> afaces = null;    // For AFD
                // For ALL FD Libs
                Integer face_detected = 0;
                BitmapFactory.Options bitmapFatoryOptions = new BitmapFactory.Options();
                Bitmap bitmap = BitmapFactory.decodeByteArray(frame,0,frame.length,bitmapFatoryOptions);
                List<Bitmap> bmfaces = new ArrayList<>();

                PicProcessController++;

                if(PicProcessController == PROCESS_FREQ) {
                    switch(choosenFD){
                        case QFD: //// FD: Qualcomm Face Detection Library, around 1s
                            long startTime1 = System.currentTimeMillis();
                            qFaceDet.setBitmap(bitmap);
                            qfaces = qFaceDet.getFaceData();
                            if(qfaces!=null)
                                face_detected = qfaces.length;
                            for(int j = 0; j < face_detected; j++){
                                FaceData face = qfaces[j];
                                Rect faceRect = face.rect;
                                Bitmap bmface = null;
                                try {
                                    bmface = Bitmap.createBitmap(bitmap, faceRect.left, faceRect.top, faceRect.right-faceRect.left, faceRect.bottom - faceRect.top);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    continue;
                                }
                                if (bmface != null) {
                                    bmfaces.add(bmface);
                                }
                            }
                            long endTime1 = System.currentTimeMillis();
                            logger.info("Qualcomm Detection Time cost: " + String.valueOf((endTime1 - startTime1) / 1000f) + " sec");
                            break;
                        case DFD:   //// FD: Dlib Face Detection Library, around 600ms
                            long startTime2 = System.currentTimeMillis();
                            dfaces = dFaceDet.detect(bitmap);
                            if(dfaces!=null)
                                face_detected = dfaces.size();
                            for (int j = 0; j < face_detected; ++j) {
                                VisionDetRet vdr = dfaces.get(j);
                                Bitmap bmface = null;
                                try {
                                    bmface = Bitmap.createBitmap(bitmap, vdr.getLeft(), vdr.getTop(), vdr.getRight() - vdr.getLeft(), vdr.getBottom() - vdr.getTop());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    continue;
                                }
                                if (bmface != null) {
                                    bmfaces.add(bmface);
                                }
                            }
                            long endTime2 = System.currentTimeMillis();
                            logger.info("Dlib Detection Time cost: " + String.valueOf((endTime2 - startTime2) / 1000f) + " sec");
                            break;
                        case AFD:   //// Android Media Face Detection Library, around 100ms
                            long startTime3 = System.currentTimeMillis();
                            afaces = aFaceDet.detect(new Frame.Builder().setBitmap(bitmap).build());
                            if(afaces!=null)
                                face_detected = afaces.size();
                            for (int j = 0; j < face_detected; ++j) {
                                Face face = afaces.valueAt(j);
                                Bitmap bmface = null;
                                try {
                                    bmface = Bitmap.createBitmap(bitmap, (int) face.getPosition().x, (int) face.getPosition().y, (int) face.getWidth(), (int) face.getHeight());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    continue;
                                }
                                if (bmface != null) {
                                    bmfaces.add(bmface);
                                }
                            }
                            long endTime3 = System.currentTimeMillis();
                            logger.info("Android Service Detection Time cost: " + String.valueOf((endTime3 - startTime3) / 1000f) + " sec");
                            break;
                        default:
                            logger.error("NO Face Detection Library!");
                    }
                    PicProcessController = 0;
                }

                logger.info("TIME STAMP 6, FACE DETECTOR FINISHES PROCESSING A FRAME, "+System.nanoTime());

                int bmfaceNum = bmfaces.size();
                if(bmfaceNum > 0){
                    for (int j = 0; j < bmfaceNum; ++j) {
                        // convert to jpg file to save storage space or transmission time
                        Bitmap bmface = bmfaces.get(j);
                        ByteArrayOutputStream stream = new ByteArrayOutputStream();
                        bmface.compress(Bitmap.CompressFormat.JPEG, 100, stream);
                        byte[] imageByteArray = stream.toByteArray();
                        InternodePacket pktSend = new InternodePacket();
                        pktSend.ID = pktRecv.ID;
                        pktSend.type = InternodePacket.TYPE_DATA;
                        pktSend.fromTask = getTaskID();
                        pktSend.complexContent = imageByteArray;
                        pktSend.traceTask = pktRecv.traceTask;
                        pktSend.traceTask.add("MFD_"+getTaskID());
                        pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                        pktSend.traceTaskEnterTime.put("MFD_"+ getTaskID(), enterTime);
                        pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                        long exitTime = System.nanoTime();
                        pktSend.traceTaskExitTime.put("MFD_"+ getTaskID(), exitTime);
                        String component = MyFaceRecognizer.class.getName();
                        try {
                            MessageQueues.emit(pktSend, getTaskID(), component);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    logger.info("TIME STAMP 7, FACE DETECTOR CATCHES FACES, "+System.nanoTime());
                }
            }
        }
    }

    @Override
    public void postExecute(){

        if(qFaceDet!=null)
        {
            qFaceDet.release();
            qFaceDet = null;
        }

        if(dFaceDet!=null)
        {
            dFaceDet.release();
            dFaceDet=null;
        }

        if(aFaceDet!=null)
        {
            aFaceDet.release();
            aFaceDet = null;
        }
    }

}
