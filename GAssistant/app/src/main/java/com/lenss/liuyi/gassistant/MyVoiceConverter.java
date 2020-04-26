package com.lenss.liuyi.gassistant;

import android.os.SystemClock;

import com.lenss.mstorm.communication.internodes.InternodePacket;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.topology.Processor;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import java.lang.ref.WeakReference;

import android.widget.TextView;

import org.kaldi.KaldiRecognizer;
import org.kaldi.Model;

import static java.lang.Math.min;

public class MyVoiceConverter extends Processor{

    static { System.loadLibrary("kaldi_jni"); }
    private Logger logger;
    private final String TAG = "MyVoiceConverter";
    private WeakReference<MyVoiceConverter> mscWeakReference;
    private Model model;
    TextView resultView;

    @Override
    public void prepare(){
        logger = Logger.getLogger(TAG);
        mscWeakReference = new WeakReference<>(this);
        mscWeakReference.get().model =
                new Model("/home/liuyi/Documents/VoiceAssist/EdgeStorm/GAssistant" +
                        "src/main/assets/sync/model-android");
    }

    @Override
    public void execute(){
        KaldiRecognizer rec = new KaldiRecognizer(mscWeakReference.get().model, 16000.f);
        while(!Thread.currentThread().interrupted()){

            StringBuilder result = new StringBuilder();
            InternodePacket pktRecv = MessageQueues.retrieveIncomingQueue(getTaskID());
            long enterTime = SystemClock.elapsedRealtimeNanos();
            if(pktRecv != null){
                logger.debug("pkt received at mySpeechConverter!");
                byte[] voiceByteArray = pktRecv.complexContent;
                logger.info("This device received voice message, " + getTaskID());
                int left = 0;
                while(left < voiceByteArray.length){
                    int right = min(left + 4096, voiceByteArray.length);
                    byte[] segmentByteArray =  ArrayUtils.subarray(voiceByteArray, left, right);
                    if(rec.AcceptWaveform(segmentByteArray, segmentByteArray.length)){
                        result.append(rec.Result());
                    }else{
                        result.append(rec.PartialResult());
                    }
                    left += 4096;
                }
                result.append(rec.FinalResult());
            }
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
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }

    }

    @Override
    public void postExecute(){
        if(mscWeakReference.get() != null){
            mscWeakReference.clear();
        }

        if(model != null){
            model.delete();
        }
    }
}
