package com.lenss.cmy.gvisionanalytics;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.SystemClock;

import com.lenss.amvp.feature.Quantizer;
import com.lenss.amvp.tflite.Classifier;
import com.lenss.amvp.tflite.Classifier.*;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.topology.Processor;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class MyFaceCommonFeature extends Processor {
    private final String TAG="MyFaceCommonFeature";
    Logger logger;
    Classifier classifier;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);

        Model modelType = Model.FLOAT;
        String modelName = "common";
        String modelVariant = getModelModelVariant();
        logger.info("ModelVariant===========" + modelVariant);
        Part part = Part.FIRST_HALF;
        Device device = Device.CPU;

        int numThreads = 1;

        try {
            classifier = Classifier.create(MStorm.getActivity(), modelType, modelName, modelVariant, part, device, numThreads);
            logger.info("Successfully load common feature extractor");
        } catch (IOException e){
            e.printStackTrace();
            logger.error("Failed to create common feature extractor");
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

                // get feature maps
                ByteBuffer featureMaps = classifier.extractFeatureMaps(scaledBitmap);
                featureMaps.rewind();

//                // no quantization
//                logger.info("MyFaceCommonFeature1=============="+featureMaps.remaining());
//                byte[] featureMapByteArray = new byte[featureMaps.remaining()];
//                featureMaps.get(featureMapByteArray);
//                logger.info("MyFaceCommonFeature2=============="+featureMapByteArray.length);

                long startQuantizationTime = SystemClock.elapsedRealtimeNanos();
                // quantization
                Quantizer quantizer = new Quantizer();
                int numOfBits = 8;
                ByteBuffer quantizedFeatureMaps = quantizer.quantize(featureMaps, numOfBits);
                quantizedFeatureMaps.rewind();
                long endQuantizationTime = SystemClock.elapsedRealtimeNanos();
                logger.info("QuantizationTime:"+(endQuantizationTime-startQuantizationTime));
                logger.info("MyFaceCommonFeature1=============="+quantizedFeatureMaps.remaining());
                byte[] featureMapByteArray = new byte[quantizedFeatureMaps.remaining()];
                quantizedFeatureMaps.get(featureMapByteArray);
                logger.info("MyFaceCommonFeature2=============="+featureMapByteArray.length);

                InternodePacket pktSend = new InternodePacket();
                pktSend.ID = pktRecv.ID;
                pktSend.type = InternodePacket.TYPE_DATA;
                pktSend.fromTask = taskID;
                pktSend.simpleContent = pktRecv.simpleContent;
                pktSend.simpleContent.put("numOfBits", String.valueOf(numOfBits));
                pktSend.simpleContent.put("minValue", String.valueOf(quantizer.minValue));
                pktSend.simpleContent.put("maxValue", String.valueOf(quantizer.maxValue));
                pktSend.complexContent = featureMapByteArray;
                pktSend.traceTask = pktRecv.traceTask;
                pktSend.traceTask.add("MFCF_"+taskID);
                pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                pktSend.traceTaskEnterTime.put("MFCF_"+taskID, enterTime);
                pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                long exitTime = SystemClock.elapsedRealtimeNanos();
                pktSend.traceTaskExitTime.put("MFCF_"+taskID, exitTime);

                String compAge = MyAgeAnalyzerFromFeature.class.getName();
                String compEmotion = MyEmotionAnalyzerFromFeature.class.getName();
                String compGender = MyGenderAnalyzerFromFeature.class.getName();
                try {
                    MessageQueues.emit(pktSend, taskID, compAge);
                    MessageQueues.emit(pktSend, taskID, compEmotion);
                    MessageQueues.emit(pktSend, taskID, compGender);
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
