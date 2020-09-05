package com.lenss.liuyi;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import android.Manifest;
import android.content.pm.PackageManager;
//import javax.sound.sampled.AudioFormat;
import android.media.AudioFormat;
import android.media.AudioRecord;
import android.media.MediaRecorder;
import android.media.MediaRecorder.AudioSource;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.os.FileObserver;
import android.os.Handler;
import android.os.Message;
import android.os.SystemClock;
import android.provider.MediaStore;
import android.speech.tts.Voice;
import android.util.Log;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import com.android.R;
import com.lenss.liuyi.GAssistantTopology;
import com.lenss.mstorm.communication.masternode.Reply;
import com.lenss.mstorm.topology.StormSubmitter;
import com.lenss.mstorm.topology.Topology;

import org.apache.log4j.Logger;
//import org.kaldi.Assets;
import org.kaldi.KaldiRecognizer;
import org.kaldi.Model;
import org.kaldi.RecognitionListener;
import org.kaldi.SpeechService;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Calendar;
import java.util.Random;

public class GAssistantActivity extends AppCompatActivity {

//    static { System.loadLibrary("kaldi_jni"); }

    private static final String TAG = "GDetectionActivity";

    private static final String apkFileName = "GAssistant.apk";

    private static final String MStormDir = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/";
    private static final String VOICE_TEXT_PATH = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/VoiceText/";
//    private static final String MStormDir = this.getExternalFilesDir().getPath()
    private static final String RAW_VOICE_URL =  MStormDir + "RawVoice/";
    private static final String STORAGE_VOICE_URL =  MStormDir + "StorageVoice/";
    private static final String LOG_URL = MStormDir + "mstorm.log";

    public static int voiceReceptorParallel = 1;
    public static int voiceConverterParallel = 1;
    public static int voiceSaverParallel = 1;

    public static int voiceReceptorScheduleReq = Topology.Schedule_Local;
    public static int voiceConverterScheduleReq = Topology.Schedule_Any;
    public static int voiceSaverScheduleReq = Topology.Schedule_Local;

    //initial stream grouping method of each component
    public static int voiceConverterGroupMethod = Topology.Shuffle;
    public static int voiceSaverGroupMethod = Topology.Shuffle;

    private static final int MSG_RTSP = 1;

    static private final int STATE_START = 0;
    static private final int STATE_READY = 1;
    static private final int STATE_FILE = 2;
    static private final int STATE_MIC = 3;

    static private final int PERMISSIONS_REQUEST_RECORD_AUDIO = 1;
    static private final int PERMISSIONS_REQUEST_WRITE_EXTERNAL_STORAGE = 1;
    static private final int PERMISSIONS_REQUEST_READ_EXTERNAL_STORAGE = 1;

    public static int topologyID = 0;
    TextView resultView;
    Logger logger;

    private AudioRecord recorder;
    private final int sample_rate = 16000;
    private final int bufferSize = Math.round(sample_rate * 0.4f);
    private int count = 1;
    private int byteCount = 0;

    Thread recognizeThread;
    boolean creatWav = true;
    private File pcmFile = null;
    private File wavFile = null;
//    public static Model model;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        resultView = findViewById(R.id.result_text);
        setUiState(STATE_START);

        int writeStoragePermissionCheck = ContextCompat.checkSelfPermission(getApplicationContext(), Manifest.permission.WRITE_EXTERNAL_STORAGE);
        if(writeStoragePermissionCheck != PackageManager.PERMISSION_GRANTED){
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE}, PERMISSIONS_REQUEST_WRITE_EXTERNAL_STORAGE);
            return;
        }

        int readStoragePermissionCheck = ContextCompat.checkSelfPermission(getApplicationContext(), Manifest.permission.READ_EXTERNAL_STORAGE);
        if(readStoragePermissionCheck != PackageManager.PERMISSION_GRANTED){
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.READ_EXTERNAL_STORAGE}, PERMISSIONS_REQUEST_READ_EXTERNAL_STORAGE);
            return;
        }

        int audioRecordPermissionCheck = ContextCompat.checkSelfPermission(getApplicationContext(), Manifest.permission.RECORD_AUDIO);
        if(audioRecordPermissionCheck != PackageManager.PERMISSION_GRANTED){
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.RECORD_AUDIO}, PERMISSIONS_REQUEST_RECORD_AUDIO);
            return;
        }

        try {
            Utils.initLogger(LOG_URL);
            logger = Logger.getLogger(TAG);
        } catch (IOException e){
//            System.out.println("init log failed");
            resultView.setText("Can not create log file probably due to insufficient permission");
//            logger.info("Can not create log file probably due to insufficient permission");
            logger.error(e);
        }

        // listener for receive voice and recognize the voice
        findViewById(R.id.recognize_mic).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try{
                    recognizeMicrophone();
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
        });

        // listener for recognize file
        findViewById(R.id.recognize_file).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                recognizeFile();
            }
        });

        // listener for submiting topology
        findViewById(R.id.action_submit_topology).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                submitTopology();
            }
        });

        // listener for canceling topology
        findViewById(R.id.action_cancel_topology).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                cancelTopology();
            }
        });

        new SetupTask(this).execute();
    }


    private static class SetupTask extends AsyncTask<Void, Void, Exception>{
        WeakReference<GAssistantActivity> activityWeakReference;

        SetupTask(GAssistantActivity activity){
            this.activityWeakReference = new WeakReference<>(activity);
        }

        @Override
        protected Exception doInBackground(Void... params){

            /*create and clean raw voice dir*/
            File rawVoiceFolder = new File(RAW_VOICE_URL);
            if(!rawVoiceFolder.exists()){
                boolean f = rawVoiceFolder.mkdirs();
                Log.d(TAG, String.format("pcm dir: %s -> %b", RAW_VOICE_URL, f));
            }else{
                for(File f: rawVoiceFolder.listFiles()){
                    boolean d = f.delete();
                    Log.d(TAG, String.format("Delete pcm file: %s -> %b", f.getName(), d));
                }
                Log.d(TAG, String.format("pcm dir: %s", RAW_VOICE_URL));
            }

            /*create and clean voice recognition result dir*/
            File voiceSaverDir = new File(VOICE_TEXT_PATH);
            if(!voiceSaverDir.exists()){
                boolean f = voiceSaverDir.mkdirs();
                Log.d(TAG, String.format("saver dir: %s -> %b", RAW_VOICE_URL, f));
            }else{
                for(File f: voiceSaverDir.listFiles()){
                    boolean d = f.delete();
                    Log.d(TAG, String.format("Delete saver file: %s -> %b", f.getName(), d));
                }
                Log.d(TAG, String.format("saver dir: %s", RAW_VOICE_URL));
            }

            /*monitor the voice recognition result text file*/
//            FileObserver fo = new FileObserver(VOICE_TEXT_PATH) {
//                @Override
//                public void onEvent(int event, @Nullable String path) {
//                    if(event == FileObserver.CLOSE_WRITE &&
//                            path.substring(path.lastIndexOf('.')).equals(".txt")){
//
//                    }
//                }
//            };
//            fo.startWatching();
//            Log.d(TAG, "File Observer is watching " + VOICE_TEXT_PATH);

            return null;
        }

        @Override
        protected void onPostExecute(Exception result){
            if(result != null){
                activityWeakReference.get().setErrorState(String.format(activityWeakReference.get().getString(R.string.failed), result));
            }else{
                activityWeakReference.get().setUiState(STATE_READY);
            }
        }
    }

    private void setErrorState(String message){
        resultView.setText(message);
        ((Button)findViewById(R.id.recognize_mic)).setText("Recognize Microphone");
        findViewById(R.id.recognize_mic).setEnabled(false);
        findViewById(R.id.recognize_file).setEnabled(false);
    }

//    private final class RecResultThread extends Thread{
//
//        @Override
//        public void run() {
//
//            while(!interrupted()){
//
//            }
//        }
//    }


    private final class RecognizerThread extends Thread{

        private int remainingSamples;
        private int timeoutSamples;
        private final static int NO_TIMEOUT = -1;

        short[] buffer = new short[bufferSize];
        byte[] bytebuffer = new byte[bufferSize*2];

        private boolean createWav = false;

        public RecognizerThread(boolean wavFlag){
            createWav = wavFlag;
            this.remainingSamples = NO_TIMEOUT;
        }

        @Override
        public void run(){
            String FileURL = RAW_VOICE_URL +  ((count++) % Integer.MAX_VALUE);
            FileOutputStream pcmFOS;
            FileOutputStream wavFOS;
            try{
                pcmFile = new File(FileURL + ".pcm");
                wavFile = new File(FileURL + ".wav");
//                pcmFile.createNewFile();
                pcmFOS = new FileOutputStream(pcmFile);
                wavFOS = new FileOutputStream(wavFile);
                if(creatWav){
//                    wavFile.createNewFile();
                    VoiceUtils.writeWavFileHeader(wavFOS, bufferSize, sample_rate, recorder.getChannelCount());
                }
//                wavOs = new FileOutputStream(wavFile);
//                byteOs = new FileOutputStream(byteFile);
//            logger.debug(TAG +  " new count( " + count +  " ) file created ");
                recorder.startRecording();
                if(recorder.getRecordingState() == AudioRecord.RECORDSTATE_STOPPED){
                    recorder.stop();
                    logger.info(" recorder stopped!");
                    return;
                }
                logger.info(" Start to record");
//            while(!interrupted() && (timeoutSamples == NO_TIMEOUT) || (remainingSamples > 0)){
                while(!interrupted()){


                    /* read in byte buffer and save in byte array*/
//                logger.debug(TAG +  " recorder begin read one buffer");
                    int nread = recorder.read(bytebuffer,0, bytebuffer.length);
//                logger.debug(TAG +  " recorder finish read one buffer");
                    pcmFOS.write(bytebuffer, 0, nread);
                    pcmFOS.flush();
                    if(createWav){
                        wavFOS.write(bytebuffer, 0, nread);
                        wavFOS.flush();
                    }

                    byteCount += nread;


                }

                recorder.stop();    //if interrupt, we stop the recording
                pcmFOS.close();
                wavFOS.close();

                if(createWav){
                    RandomAccessFile wavRaf = new RandomAccessFile(FileURL + ".wav", "rw");
                    byte[] header = VoiceUtils.generateWavFileHeader(pcmFile.length(), sample_rate, recorder.getChannelCount());
                    logger.info("header: " + VoiceUtils.getHexString(header));
                    wavRaf.seek(0);
                    wavRaf.write(header);
                    wavRaf.close();
                    logger.info("wavFile.length: " + wavFile.length());
                }
                logger.info("pcmFile.length: " + pcmFile.length());
            } catch (Exception e){
                logger.info("AudioThread error");
            }

            logger.info("come out of while loop, stopped recording.");
        }
    }

    private void recognizeMicrophone() throws IOException{

        if(recorder == null){
            recorder = new AudioRecord(
                    AudioSource.VOICE_RECOGNITION,
                    sample_rate,
                    AudioFormat.CHANNEL_IN_MONO,
                    AudioFormat.ENCODING_PCM_16BIT,
                    bufferSize * 2);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(recorder.getRecordingState() == AudioRecord.STATE_UNINITIALIZED){
                recorder.release();
                throw new IOException("Failed to initialize recorder. Microphone might be already in use");
            }
        }

        if(recognizeThread != null){
            setUiState(STATE_READY);
            try {

                logger.info(" interrupting the recognize thread");
                recognizeThread.interrupt();
                logger.info(" interrupted the recognize thread");

                logger.info(" joining the recognize thread");
                recognizeThread.join(1000);
                logger.info(" joined the recognize thread");

            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
            recognizeThread = null;
        }else{
            setUiState(STATE_MIC);
            recognizeThread = new RecognizerThread(creatWav);
            recognizeThread.start();
        }
    }

    private void setUiState(int state){
        switch (state){
            case STATE_START:
                resultView.setText("Preparing the recognizer ");
                findViewById(R.id.recognize_file).setEnabled(false);
                findViewById(R.id.recognize_mic).setEnabled(false);
                break;
            case STATE_READY:
                resultView.setText("Ready ");
                ((Button) findViewById(R.id.recognize_mic)).setText("Recognize Microphone");
                findViewById(R.id.recognize_file).setEnabled(true);
                findViewById(R.id.recognize_mic).setEnabled(true);
                break;
            case STATE_FILE:
                resultView.append("Starting ");
                findViewById(R.id.recognize_mic).setEnabled(false);
                findViewById(R.id.recognize_file).setEnabled(false);
                break;
            case STATE_MIC:
                ((Button)findViewById(R.id.recognize_mic)).setText("Stop Microphone");
                findViewById(R.id.recognize_file).setEnabled(false);
                findViewById(R.id.recognize_mic).setEnabled(true);
                break;
        }
    }

    public void submitTopology(){
        logger.info("======================= enter into submitTopology once again ===============\n");
        if(topologyID != 0){
//            logger.info("======================= topologyID != 0 ===============\n");
            Toast.makeText(this, "Topology", Toast.LENGTH_SHORT).show();
//            logger.info("======================= enter into submitTopology once again ===============\n");
        }else{
//            logger.info("======================= topologyID == 0 ===============\n");
            Topology topology = GAssistantTopology.createTopology();
            StormSubmitter submitter = new StormSubmitter(this);
            if(!submitter.isReady()){
                logger.info("======================= submitter is not ready ===============\n");
                mhandler.obtainMessage(MSG_RTSP, "EdgeKeeper or MStorm master does NOT start yet").sendToTarget();
                return;
            }
            logger.info("============= Submitter is ready ================");
            submitter.submitTopology(apkFileName, topology);

            // wait for reply containing topologyID
            Reply reply;
            while((reply = submitter.getReply()) == null){
                try {
                    Thread.sleep(100);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            topologyID = new Integer(reply.getContent());
            if(topologyID != 0){
                Toast.makeText(this, "Topology Scheduled!", Toast.LENGTH_SHORT).show();
            }else{
                Toast.makeText(this, "Topology can NOT be scheduled! No enough computing nodes!", Toast.LENGTH_SHORT).show();
            }
        }
    }

    public void cancelTopology(){
        if(topologyID == 0){
            Toast.makeText(this, "Topology Already Canceled!", Toast.LENGTH_SHORT).show();
        }else{
            StormSubmitter submitter = new StormSubmitter(this);
            if(!submitter.isReady()){
                mhandler.obtainMessage(MSG_RTSP, "EdgeKeeper does not work").sendToTarget();
                return;
            }
            logger.info("============= Submitter is ready ================");
            submitter.cancelTopology(Integer.toString(topologyID));

            topologyID = 0;
            Toast.makeText(this, "Topology Canceled!", Toast.LENGTH_SHORT).show();
        }
    }

//    private static class RecognizeFileTask extends AsyncTask<Void, Void, String>{
//
//        WeakReference<GAssistantActivity> activityWeakReference;
//        WeakReference<TextView> resultView;
//
//        RecognizeFileTask(GAssistantActivity activity, TextView resultView){
//            this.activityWeakReference = new WeakReference<GAssistantActivity>(activity);
//            this.resultView = new WeakReference<>(resultView);
//        }
//
//        @Override
//        protected String doInBackground(Void... params){
//            KaldiRecognizer rec;
//            long startTime = System.currentTimeMillis();
//            StringBuilder result = new StringBuilder();
//            try{
//                rec = new KaldiRecognizer(activityWeakReference.get().model, 16000.f);
//                InputStream ais = activityWeakReference.get().getAssets().open("10001-90210-01803.wav");
//                if(ais.skip(44) != 44){
//                    return "";
//                }
//                byte[] b = new byte[4096];
//                int nbytes;
//                while((nbytes = ais.read(b)) >= 0){
//                    if(rec.AcceptWaveform(b, nbytes)){
//                        result.append(rec.Result());
//                    } else {
//                        result.append(rec.PartialResult());
//                    }
//                }
//                result.append(rec.FinalResult());
//            }catch (IOException e){
//                return "";
//            }
//            return String.format(activityWeakReference.get().getString(R.string.elapsed), result.toString(), (System.currentTimeMillis() - startTime));
//        }
//
//        @Override
//        protected void onPostExecute(String result){
//            activityWeakReference.get().setUiState(STATE_READY);
//            resultView.get().append(result + "\n");
//        }
//    }

    private void recognizeFile(){
        setUiState(STATE_FILE);
//        new RecognizeFileTask(this, resultView).execute();
    }

    private final Handler mhandler = new Handler(){
        @Override
        public void handleMessage(Message msg){
            switch (msg.what) {
                case MSG_RTSP:
                    Toast.makeText(GAssistantActivity.this, msg.obj.toString(), Toast.LENGTH_SHORT).show();
                    break;
            }
        }
    };
}
