package com.lenss.cmy.gvisionanalytics;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.SystemClock;

import com.lenss.amvp.tflite.Classifier;
import com.lenss.amvp.tflite.Classifier.*;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.topology.Processor;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class MyGenderAnalyzer extends Processor {
    private final String TAG="MyGenderAnalyzer";
    Logger logger;
    Classifier classifier;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);

        Model modelType = Model.FLOAT;
        String modelName = "gender";
        String modelVariant = "mobileNetV2_frozen45";
        Part part = Part.WHOLE;
        Device device = Device.CPU;
        int numThreads = 1;

        try {
            classifier = Classifier.create(MStorm.getActivity(), modelType, modelName, modelVariant, part, device, numThreads);
        } catch (IOException e){

            logger.error("Failed to create gender classifier");
        }
    }

    @Override
    public void execute(){
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
            if(pktRecv != null){
                long enterTime = SystemClock.elapsedRealtimeNanos();
                byte[] frame = pktRecv.complexContent;
                BitmapFactory.Options bitmapFatoryOptions = new BitmapFactory.Options();
                Bitmap bitmap = BitmapFactory.decodeByteArray(frame,0,frame.length,bitmapFatoryOptions);
                Bitmap scaledBitmap = Bitmap.createScaledBitmap(bitmap, classifier.getImageSizeX(), classifier.getImageSizeY(),true);
                logger.debug("===== GenderAnalyzer received a frame ====="+ taskID);

                // get recognition result and confidence
                final List<Classifier.Recognition> results = classifier.recognizeImage(scaledBitmap);
                Classifier.Recognition recognition;
                String recognitionResult = "X";
                if(results!=null){
                    recognition = results.get(0);
                    if(recognition!=null){
                        recognitionResult = recognition.getTitle();
                    }
                }

                // send the result to the next component
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                bitmap.compress(Bitmap.CompressFormat.JPEG, 100, stream);
                byte[] imageByteArray = stream.toByteArray();
                InternodePacket pktSend = new InternodePacket();
                pktSend.ID = pktRecv.ID;
                pktSend.type = InternodePacket.TYPE_DATA;
                pktSend.fromTask = taskID;
                pktSend.simpleContent.put("gender",recognitionResult);
                pktSend.complexContent = imageByteArray;

                pktSend.traceTask = pktRecv.traceTask;
                pktSend.traceTask.add("MGA_"+taskID);
                pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                pktSend.traceTaskEnterTime.put("MGA_"+taskID, enterTime);
                pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                long exitTime = SystemClock.elapsedRealtimeNanos();
                pktSend.traceTaskExitTime.put("MGA_"+taskID, exitTime);

                String component = MyFaceSaver.class.getName();
                try {
                    MessageQueues.emit(pktSend, taskID, component);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    @Override
    public void postExecute(){
        if(classifier!=null)
            classifier.close();
    }
}
