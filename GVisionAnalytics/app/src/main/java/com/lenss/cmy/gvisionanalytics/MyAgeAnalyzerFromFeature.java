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

import it.unimi.dsi.fastutil.bytes.Byte2ByteArrayMap;

public class MyAgeAnalyzerFromFeature extends Processor {
    private final String TAG="MyAgeAnalyzerFromFeature";
    Logger logger;
    Classifier classifier;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);

        Model modelType = Model.FLOAT;
        String modelName = "age";
        String modelVariant = getModelModelVariant();
        logger.info("ModelVariant===========" + modelVariant);
        Part part = Part.SECOND_HALF;
        Device device = Device.CPU;

        int numThreads = 1;

        try {
            classifier = Classifier.create(MStorm.getActivity(), modelType, modelName, modelVariant, part, device, numThreads);
        } catch (IOException e){
            e.printStackTrace();
            logger.error("Failed to create age classifier");
        }
    }

    @Override
    public void execute(){
        int taskID = getTaskID();
        while (!Thread.currentThread().isInterrupted()) {
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(taskID);
            if(pktRecv != null){
                long enterTime = SystemClock.elapsedRealtimeNanos();
                byte[] featureMapByteArray = pktRecv.complexContent;
                logger.info("===== MyAgeAnalyzerFromFeature received feature maps ====="+ featureMapByteArray.length);

                ByteBuffer featureMaps = ByteBuffer.wrap(featureMapByteArray);
                featureMaps.rewind();

                long startDeQuantizationTime = SystemClock.elapsedRealtimeNanos();
                // dequantization
                int numOfBits = Integer.parseInt(pktRecv.simpleContent.get("numOfBits"));
                float minValue = Float.parseFloat(pktRecv.simpleContent.get("minValue"));
                float maxValue = Float.parseFloat(pktRecv.simpleContent.get("maxValue"));
                logger.info("Parameters for dequatization ===" + numOfBits + "," + minValue + "," + maxValue);
                Quantizer quantizer = new Quantizer();
                ByteBuffer dequantizedFeatureMaps = quantizer.dequantize(featureMaps, numOfBits, minValue, maxValue);
                dequantizedFeatureMaps.rewind();
                logger.info("===== MyAgeAnalyzerFromFeature dequantizedFeatureMaps ====="+ dequantizedFeatureMaps.remaining());
                long endDeQuantizationTime = SystemClock.elapsedRealtimeNanos();
                logger.info("MyAgeAnalyzerFromFeature DeQuantizationT:"+(endDeQuantizationTime-startDeQuantizationTime));

                // get recognition result and confidence
                final List<Recognition> results = classifier.recognizeFeatureMaps(dequantizedFeatureMaps);
                Recognition recognition;
                String recognitionResult = "X";
                if(results!=null){
                    recognition = results.get(0);
                    if(recognition!=null){
                        recognitionResult = recognition.getTitle();
                    }
                }

                String log = "";
                for (Recognition recog: results){
                    log += recog.getTitle() + ":" + recog.getConfidence() + ";";
                }

                logger.info("MyAgeAnalyzerFromFeature result=============="+log);

                // send the result to the next component
                InternodePacket pktSend = new InternodePacket();
                pktSend.ID = pktRecv.ID;
                pktSend.type = InternodePacket.TYPE_DATA;
                pktSend.fromTask = taskID;
                pktSend.simpleContent = pktRecv.simpleContent;
                pktSend.simpleContent.put("age",recognitionResult);
                pktSend.traceTask = pktRecv.traceTask;
                pktSend.traceTask.add("MAAFF_"+taskID);
                pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                pktSend.traceTaskEnterTime.put("MAAFF_"+taskID, enterTime);
                pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                long exitTime = SystemClock.elapsedRealtimeNanos();
                pktSend.traceTaskExitTime.put("MAAFF_"+taskID, exitTime);
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
