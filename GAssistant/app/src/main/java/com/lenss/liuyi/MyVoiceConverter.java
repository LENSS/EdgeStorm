package com.lenss.liuyi;

import android.os.SystemClock;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;

import org.apache.log4j.Logger;
import java.util.Arrays;

import org.kaldi.KaldiRecognizer;
import org.kaldi.Model;

import static java.lang.Math.min;

public class MyVoiceConverter extends Processor{

    private Logger logger;
    private static String TAG = "MyVoiceConverter";
    private Model converter_model;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);
        converter_model = new Model("/storage/emulated/0/Android/data/files/sync/model-android");
        logger.info("Model initialized in VoiceConverter");
    }

    @Override
    public void execute(){

        KaldiRecognizer rec = new KaldiRecognizer(converter_model, 16000.f);
        StringBuilder result = new StringBuilder();
        while(!Thread.currentThread().interrupted()){

            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            long enterTime = SystemClock.elapsedRealtimeNanos();
            if(pktRecv != null){
                byte[] voiceByteArray = pktRecv.complexContent;
                logger.info(String.format("received %d bytes from %d", voiceByteArray.length, getTaskID()));
//                if(rec.AcceptWaveform(voiceByteArray, voiceByteArray.length)){
//                    result.append(rec.Result());
//                } else {
//                    result.append(rec.PartialResult());
//                }
//                logger.info("This device received voice message, " + getTaskID());
                int left = 0;
                while(left < voiceByteArray.length){
                    int right = min(left + 4096, voiceByteArray.length);
                    byte[] segmentByteArray =  Arrays.copyOfRange(voiceByteArray, left, right);
                    if(rec.AcceptWaveform(segmentByteArray, segmentByteArray.length)){
                        result.append(rec.Result());
                    }else{
                        result.append(rec.PartialResult());
                    }
                    left = right;
                }
                result.append(rec.FinalResult());
                logger.info("kaldi recognizer get the result: " + result.toString());

                /*prepare the packet to send from voice converter*/
                byte[] textResultByteArray = result.toString().getBytes();
                InternodePacket pktSend = new InternodePacket();
                pktSend.ID = pktRecv.ID;
                pktSend.type = InternodePacket.TYPE_DATA;
                pktSend.fromTask = getTaskID();
                pktSend.complexContent = textResultByteArray;
                pktSend.traceTask = pktRecv.traceTask;
                pktSend.traceTask.add("MSC_" + getTaskID());
                pktSend.traceTaskEnterTime = pktRecv.traceTaskEnterTime;
                pktSend.traceTaskEnterTime.put("MSC_" + getTaskID(), enterTime);
                pktSend.traceTaskExitTime = pktRecv.traceTaskExitTime;
                long exitTime = SystemClock.elapsedRealtimeNanos();
                pktSend.traceTaskExitTime.put("MSC_" + getTaskID(), exitTime);
                String component = MyVoiceSaver.class.getName();
                try{
                    MessageQueues.emit(pktSend, getTaskID(), component);
                    logger.debug(String.format("Converter sent %d bytes", pktSend.complexContent.length));
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void postExecute(){
        if(converter_model != null){
            converter_model.delete();
        }
    }
}
