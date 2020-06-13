package com.lenss.cmy.gvisionanalytics;

import android.os.SystemClock;

import com.lenss.amvp.feature.Quantizer;
import com.lenss.amvp.tflite.Classifier;
import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.topology.Processor;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class MyEmotionAnalyzerFromFeature extends Processor {
    private final String TAG="MyEmotionAnalyzerFromFeature";
    Logger logger;
    Classifier classifier;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);

        Classifier.Model modelType = Classifier.Model.FLOAT;
        String modelName = "emotion";
        String modelVariant = getModelModelVariant();
        logger.info("ModelVariant===========" + modelVariant);
        Classifier.Part part = Classifier.Part.SECOND_HALF;
        Classifier.Device device = Classifier.Device.CPU;

        int numThreads = 1;

        try {
            classifier = Classifier.create(MStorm.getActivity(), modelType, modelName, modelVariant, part, device, numThreads);
        } catch (IOException e){
            e.printStackTrace();
            logger.error("Failed to create emotion classifier");
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
                logger.info("===== MyEmotionAnalyzerFromFeature received feature maps ====="+ featureMapByteArray.length);

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
                logger.info("===== MyEmotionAnalyzerFromFeature dequantized FeatureMaps ====="+ dequantizedFeatureMaps.remaining());
                long endDeQuantizationTime = SystemClock.elapsedRealtimeNanos();
                logger.info("MyEmotionAnalyzerFromFeature DeQuantizationT:"+(endDeQuantizationTime-startDeQuantizationTime));

                // get recognition result and confidence
                final List<Classifier.Recognition> results = classifier.recognizeFeatureMaps(dequantizedFeatureMaps);
                Classifier.Recognition recognition;
                String recognitionResult = "X";
                if(results!=null){
                    recognition = results.get(0);
                    if(recognition!=null){
                        recognitionResult = recognition.getTitle();
                    }
                }

                String log = "";
                for (Classifier.Recognition recog: results){
                    log += recog.getTitle() + ":" + recog.getConfidence() + ";";
                }

                logger.info("MyEmotionAnalyzerFromFeature result=============="+log);

                // send the result to the next component
                InternodePacket pktSend = new InternodePacket();
                pktSend.ID = pktRecv.ID;
                pktSend.type = InternodePacket.TYPE_DATA;
                pktSend.fromTask = taskID;
                pktSend.simpleContent = pktRecv.simpleContent;
                pktSend.simpleContent.put("emotion",recognitionResult);
                pktSend.traceTask = pktRecv.traceTask;
                pktSend.traceTask.add("MEAFF_"+taskID);
                pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                pktSend.traceTaskEnterTime.put("MEAFF_"+taskID, enterTime);
                pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                long exitTime = SystemClock.elapsedRealtimeNanos();
                pktSend.traceTaskExitTime.put("MEAFF_"+taskID, exitTime);
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
