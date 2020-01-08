package com.lenss.cmy;

import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;
import android.os.SystemClock;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.Button;
import android.widget.EditText;
import android.widget.GridView;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;

import com.android.R;
import com.lenss.cmy.gdetection.GDetectorTopology;
import com.arthenica.mobileffmpeg.FFmpeg;
import com.lenss.mstorm.communication.masternode.Reply;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.topology.StormSubmitter;
import com.tzutalin.dlib.Constants;
import com.tzutalin.dlib.FaceRec;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Executable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class GDetectionActivity extends AppCompatActivity{ //ActionBarActivity
    public static final String TAG = "GDetectionActivity";

    private boolean mRecordingEnabled = false;      // controls button state

    // For display images
    private GridView mGridView;
    private ProgressBar mProgressBar;
    private GridViewAdapter mGridAdapter;
    private ArrayList<GridItem> mGridData;

    private static final String MStormDir = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/";
    private static final String LOG_URL = MStormDir + "mstorm.log";
    private static final String RAW_PIC_URL = MStormDir+"RawPic/";
    private static final String PIC_URL = MStormDir+"StreamFDPic/";
    private static final String STORAGE_PIC_URL = MStormDir+"StoragePic/";
    private static final String apkFileDirectory = MStormDir + "APK/";
    private static final String apkFileName = "GDetection.apk";

    private static final String CAMERA_SOURCE = "0";
    private static final String VIDEO_SOURCE = "1";
    private static String IP_CAMERA_SOURCE = "rtsp://10.8.162.1:8554/police1";

    private static final int MSG_NUM_OF_FACES=0;
    private static final int MSG_RTSP=1;

    private TextView result;

    // initial parallelism of face detector and recognizer component
    public static int myDistributorParallel = 1;
    public static int faceDetectorParallel = 1;
    public static int faceSaverParallel = 1;

    // initial stream grouping method of each component
    public static int faceDetectorGroupingMethod = Topology.Shuffle;
    public static int faceSaverGroupingMethod = Topology.Shuffle;

    // schedule requirements of each component
    public static int myDistributorScheduleReq = Topology.Schedule_Local;
    public static int faceDetectorScheduleReq = Topology.Schedule_Any;
    public static int faceSaverScheduleReq = Topology.Schedule_Local;

    public static int topologyID = 0;

    // Input frame rate
    public static String frameRate = "1";

    public static ReadFromYiCamera rfc = null;
    public static ReadFromVideo rfv = null;
    public static ReadFromRTSPCamera rfipc = null;
    public static volatile boolean noFFmpeg = true;  // variable to ensure there is only one FFmpeg running


    public static String streamSource = null;

    Logger logger;

    // overrides the methods in Activity
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_camera_capture);

        mGridView = (GridView) findViewById(R.id.gridView);
        mProgressBar = (ProgressBar) findViewById(R.id.progressBar);

        //Initialize with empty data
        mGridData = new ArrayList<GridItem>();
        mGridAdapter = new GridViewAdapter(this, R.layout.grid_item_layout, mGridData);
        mGridView.setAdapter(mGridAdapter);

        //Grid view click event
        mGridView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            public void onItemClick(AdapterView<?> parent, View v, int position, long id) {
                //Get item at position
                GridItem item = (GridItem) parent.getItemAtPosition(position);

                Intent intent = new Intent(GDetectionActivity.this, DetailsActivity.class);

                // Interesting data to pass across are the thumbnail size/location, the
                // resourceId of the source bitmap, the picture description, and the
                // orientation (to avoid returning back to an obsolete configuration if
                // the device rotates again in the meantime)

                //Pass the image title and url to DetailsActivity
                intent.putExtra("title", item.getTitle()).
                        putExtra("image", item.getImage());

                //Start details activity
                startActivity(intent);
            }
        });

        result=(TextView)findViewById(R.id.result);
        result.setText("No Faces Detected!");

        // Configure the logger to store logs in file
        try {
            Utils.initLogger(LOG_URL);
            logger = Logger.getLogger(TAG);
        } catch (IOException e) {
            result.setText("Can not create log file probably due to insufficient permission");
            logger.error(e);
        }

        // Create the folder for storing the raw pictures
        File rawPicFolder = new File(RAW_PIC_URL);
        rawPicFolder.mkdir();

        final Handler handler = new Handler();
        handler.postDelayed(new Runnable() {
            @Override
            public void run() {
                new AsyncShowPicTask().execute(PIC_URL);
                mProgressBar.setVisibility(View.VISIBLE);
                handler.postDelayed(this, 500);
            }
        }, 500);
    }

    @Override
    protected void onStart() {
        super.onStart();
        // bind to Service
        Intent intent = new Intent(this, OnClearFromRecentService.class);
        startService(intent);
    }

    @Override
    protected void onResume() {
        super.onResume();
        updateControls();
    }

    @Override
    protected void onStop(){
        logger.info("onStop");
        super.onStop();
    }

    protected void onDestroy() {
        logger.info("onDestroy");
        stopPullStream();
        cancelTopology();
        super.onDestroy();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        if(id == R.id.action_set_parallelTasks) {
            final EditText fdBox = new EditText(this);
            fdBox.setHint("faceDetector:"+faceDetectorParallel);

            final LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            layout.addView(fdBox);

            new AlertDialog.Builder(this)
                    .setView(layout)
                    .setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    try {
                        faceDetectorParallel = Integer.parseInt(fdBox.getText().toString());
                        if(faceDetectorParallel<=0 || faceDetectorParallel>10){
                            faceDetectorParallel = 1;
                            Toast.makeText(GDetectionActivity.this, "Set an integer between 1 and 10!", Toast.LENGTH_SHORT).show();
                        }
                    } catch(NumberFormatException nfe){
                        faceDetectorParallel = 1;
                        Toast.makeText(GDetectionActivity.this, "Set an integer between 1 and 10!", Toast.LENGTH_SHORT).show();
                    }
                    fdBox.setHint("faceDetector:"+faceDetectorParallel);
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    faceDetectorParallel = 1;
                    fdBox.setHint("faceDetector:"+faceDetectorParallel);
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else if (id == R.id.action_set_groupingMethod) {
            CharSequence[] streamSources = {"Shuffle", "MinSojourn", "SojournProb", "MinEWT"};
            final LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            new AlertDialog.Builder(this)
                    .setSingleChoiceItems(streamSources, 0, null)
                    .setPositiveButton("OK", new DialogInterface.OnClickListener() {
                        public void onClick(DialogInterface dialog, int whichButton) {
                            dialog.dismiss();
                            int selectedPosition = ((AlertDialog)dialog).getListView().getCheckedItemPosition();
                            switch (selectedPosition) {
                                case 0: // shuffle
                                    faceDetectorGroupingMethod = Topology.Shuffle;
                                    break;
                                case 1: // MinSojourn
                                    faceDetectorGroupingMethod = Topology.MinSojournTime;
                                    break;
                                case 2: // SojournProb
                                    faceDetectorGroupingMethod = Topology.SojournTimeProb;
                                    break;
                                case 3: // MinEWT
                                    faceDetectorGroupingMethod = Topology.MinEWT;
                                    break;
                            }
                        }
                    }).show();
        } else if(id == R.id.action_set_streamSource){
            CharSequence[] streamSources = {"Yi Camera", "Local Video", "RTSP Camera"};
            final LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            new AlertDialog.Builder(this)
                    .setSingleChoiceItems(streamSources, 0, null)
                    .setPositiveButton("OK", new DialogInterface.OnClickListener() {
                        public void onClick(DialogInterface dialog, int whichButton) {
                            dialog.dismiss();
                            int selectedPosition = ((AlertDialog)dialog).getListView().getCheckedItemPosition();
                            switch (selectedPosition) {
                                case 0: // Yi Camera
                                    streamSource = CAMERA_SOURCE;
                                    break;
                                case 1: // Local Video
                                    streamSource = VIDEO_SOURCE;
                                    break;
                                case 2: // RTSP Camera
                                    final EditText ssBox = new EditText(GDetectionActivity.this);
                                    ssBox.setHint("Input an RTSP URL, e.g.," + IP_CAMERA_SOURCE);
                                    layout.addView(ssBox);
                                    AlertDialog.Builder alert = new AlertDialog.Builder(GDetectionActivity.this);
                                    alert.setView(layout);
                                    alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                                    public void onClick(DialogInterface dialog, int which) {
                                        streamSource = ssBox.getText().toString();
                                        String validResult = isValidRTSPURL(streamSource);
                                        if(!validResult.equals("valid")){
                                            Toast.makeText(GDetectionActivity.this, validResult, Toast.LENGTH_SHORT).show();
                                            streamSource = null;
                                        }
                                    }}).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                                        public void onClick(DialogInterface dialog, int which) {
                                            streamSource = null;
                                        }
                                    }).setIcon(android.R.drawable.ic_dialog_alert).show();
                                    break;
                            }
                        }
                    }).show();
        } else if(id == R.id.action_set_frameRate){
            CharSequence[] frameRates = {"1 Frame/s", "2 Frames/s", "3 Frames/s", "5 Frames/s", "Manually Setting"};
            final LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            new AlertDialog.Builder(this)
                    .setSingleChoiceItems(frameRates, 0, null)
                    .setPositiveButton("OK", new DialogInterface.OnClickListener() {
                        public void onClick(DialogInterface dialog, int whichButton) {
                            dialog.dismiss();
                            int selectedPosition = ((AlertDialog)dialog).getListView().getCheckedItemPosition();
                            switch (selectedPosition) {
                                case 0: // 1 Frame/s
                                    frameRate = "1";
                                    break;
                                case 1: // 2 Frames/s
                                    frameRate = "2";
                                    break;
                                case 2: // 3 Frames/s
                                    frameRate = "3";
                                    break;
                                case 3: // 5 Frame/s
                                    frameRate = "5";
                                    break;
                                case 4: // Manually Setting
                                    final EditText frBox = new EditText(GDetectionActivity.this);
                                    frBox.setHint("Default (F/s): " + frameRate);
                                    layout.addView(frBox);
                                    new AlertDialog.Builder(GDetectionActivity.this)
                                            .setView(layout)
                                            .setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                                        public void onClick(DialogInterface dialog, int which) {
                                            try {
                                                frameRate = frBox.getText().toString();
                                                int fr = Integer.parseInt(frameRate);    // check if it is an int
                                                if(fr >15 || fr <= 0){
                                                    frameRate = "1";
                                                    Toast.makeText(GDetectionActivity.this, "Set an integer between 1 and 15!", Toast.LENGTH_SHORT).show();
                                                }
                                            } catch(NumberFormatException nfe){
                                                frameRate = "1";
                                                Toast.makeText(GDetectionActivity.this, "Set an integer between 1 and 15!", Toast.LENGTH_SHORT).show();
                                            }
                                            frBox.setHint("Default (F/s): " + frameRate);
                                        }
                                    }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                                        public void onClick(DialogInterface dialog, int which) {
                                            frameRate = "1";
                                            frBox.setHint("Default (F/s): " + frameRate);
                                        }
                                    }).setIcon(android.R.drawable.ic_dialog_alert).show();
                                    break;
                            }
                        }
                    }).show();
        } else if(id == R.id.action_submit_topology) {
            submitTopology();
        } else if (id == R.id.action_cancel_topology){
            cancelTopology();
        } else {
            saveImages();
        }
        return super.onOptionsItemSelected(item);
    }

    public void submitTopology(){
        if (topologyID != 0) {
            Toast.makeText(this, "Topology Already Submitted!", Toast.LENGTH_SHORT).show();
        } else {
            Topology topology = GDetectorTopology.createTopology();
            StormSubmitter submitter = new StormSubmitter(this);
            if(!submitter.isReady()){
                mHandler.obtainMessage(MSG_RTSP,"EdgeKeeper or MStorm master does NOT start yet").sendToTarget();
                return;
            }
            logger.debug("============= Submitter is ready ================");
            submitter.submitTopology(apkFileName, topology);

            // wait for reply containing topologyID
            Reply reply;
            while ((reply = submitter.getReply()) == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            topologyID = new Integer(reply.getContent());

            if (topologyID != 0)
                Toast.makeText(this, "Topology Scheduled!", Toast.LENGTH_SHORT).show();
            else
                Toast.makeText(this, "Topology can NOT be scheduled!", Toast.LENGTH_SHORT).show();
        }
    }

    public void cancelTopology(){
        if (topologyID == 0) {
            Toast.makeText(this, "Topology Already Canceled!", Toast.LENGTH_SHORT).show();
        } else {
            StormSubmitter submitter = new StormSubmitter(this);
            if(!submitter.isReady()){
                mHandler.obtainMessage(MSG_RTSP,"EdgeKeeper does not work").sendToTarget();
                return;
            }
            logger.debug("============= Submitter is ready ================");
            submitter.cancelTopology(Integer.toString(topologyID));

            topologyID = 0;
            Toast.makeText(this, "Topology Canceled!", Toast.LENGTH_SHORT).show();
        }
    }

    public void clickToggleRecording(@SuppressWarnings("unused") View unused) {
        if(streamSource == null){
            Toast.makeText(GDetectionActivity.this, "Please set stream source first!", Toast.LENGTH_SHORT).show();
            return;
        }

        mRecordingEnabled = !mRecordingEnabled;
        updateControls();
        if (mRecordingEnabled)
            startPullStream();
        else
            stopPullStream();
    }

    public void updateControls() {
        Button toggleRelease = (Button) findViewById(R.id.toggleRecording_button);
        int id = mRecordingEnabled ?
                R.string.toggleRecordingOff : R.string.toggleRecordingOn;
        toggleRelease.setText(id);
    }

    public void startPullStream(){
        if(streamSource!=null) {
            if (streamSource.equals(CAMERA_SOURCE)) { // pull stream from camera
                if (rfc == null) {
                    rfc = new ReadFromYiCamera();
                    new Thread(rfc).start();
                }
            } else if (streamSource.equals(VIDEO_SOURCE)) { // pull stream from video
                if (rfv == null) {
                    rfv = new ReadFromVideo();
                    new Thread(rfv).start();
                }
            } else {
                if (rfipc == null) {
                    rfipc = new ReadFromRTSPCamera();
                    new Thread(rfipc).start();
                }
            }
        }
    }

    public void stopPullStream(){
        if(streamSource!=null){
            if(streamSource.equals(CAMERA_SOURCE)) { // pull stream from camera
                if (rfc != null) {
                    rfc.stop();
                    rfc = null;
                }
            } else if(streamSource.equals(VIDEO_SOURCE)){ // stop pulling from video
                if (rfv != null) {
                    rfv.stop();
                    rfv = null;
                }
            } else {
                if (rfipc != null) {
                    rfipc.stop();
                    rfipc = null;
                }
            }
        }
    }

    private final Handler mHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            switch (msg.what){
                case MSG_NUM_OF_FACES:
                    result.setText(msg.obj.toString());
                    break;
                case MSG_RTSP:
                    Toast.makeText(GDetectionActivity.this, msg.obj.toString(), Toast.LENGTH_SHORT).show();
                    break;
            }

        }
    };

    class ReadFromRTSPCamera implements Runnable{
        int lastReturnCode = 1;
        boolean finished = false;

        public void run(){
            String rtspURL = streamSource;

            while (!finished && lastReturnCode==1) {
                if(noFFmpeg){
                    try{
                        noFFmpeg = false;
                        lastReturnCode = FFmpeg.execute("-i " + rtspURL +" -qscale:v 3 -r " + frameRate + " " + RAW_PIC_URL + "/sample%d.jpg"); // blocking call
                    } catch (Exception e){
                        e.printStackTrace();
                    } finally {
                        noFFmpeg = true;
                        logger.debug("noFFmpeg is set to " + noFFmpeg);
                    }
                }
                // 1: The camera has not started recording yet, there is no stream
                if(lastReturnCode==1) {
                    mHandler.obtainMessage(MSG_RTSP,"No RTSP stream!").sendToTarget();
                }
                // 0: The camera stops recording and causes timeout
                if(lastReturnCode == 0) {
                    mHandler.obtainMessage(MSG_RTSP,"RTSP server timeout").sendToTarget();
                    lastReturnCode = 1;
                }
                // wait for the camera to start recording
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

        public void stop(){
            finished = true;
            FFmpeg.cancel();
            mHandler.obtainMessage(MSG_RTSP,"Stop pulling stream!").sendToTarget();
        }
    }

    class ReadFromYiCamera implements Runnable{
        int lastReturnCode = 1;
        boolean finished = false;

        public void run(){
            String rtspIP;
            while ((rtspIP = getCameraIP()) == null ) {   // camera is not open yet
                mHandler.obtainMessage(MSG_RTSP,"Camera is not open").sendToTarget();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            while (!finished && lastReturnCode==1) {
                if(noFFmpeg){
                    try{
                        noFFmpeg = false;
                        lastReturnCode = FFmpeg.execute("-i rtsp://" + rtspIP + "/live -qscale:v 3 -r " + frameRate + " " + RAW_PIC_URL + "/sample%d.jpg"); // blocking call
                    } catch (Exception e){
                        e.printStackTrace();
                    } finally {
                        noFFmpeg = true;
                        logger.debug("noFFmpeg is set to " + noFFmpeg);
                    }
                }
                // 1: The camera has not started recording yet, there is no stream
                if(lastReturnCode==1) {
                    mHandler.obtainMessage(MSG_RTSP,"No RTSP stream!").sendToTarget();
                }
                // 0: The camera stops recording and causes timeout
                if(lastReturnCode == 0) {
                    mHandler.obtainMessage(MSG_RTSP,"RTSP server timeout").sendToTarget();
                    lastReturnCode = 1;
                }
                // wait for the camera to start recording
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

        public void stop(){
            finished = true;
            FFmpeg.cancel();
            mHandler.obtainMessage(MSG_RTSP,"Stop pulling stream!").sendToTarget();
        }
    }

    class ReadFromVideo implements Runnable{
        String videoPath = MStormDir + "Videos/test.mp4";
        int lastReturnCode = 1;
        public void run(){
            if(noFFmpeg){
                try{
                    noFFmpeg = false;
                    lastReturnCode = FFmpeg.execute("-i " +  videoPath + " -qscale:v 3 -r " + frameRate + " " + RAW_PIC_URL + "/sample%d.jpg");
                } catch (Exception e){
                    e.printStackTrace();
                } finally {
                    noFFmpeg = true;
                    logger.debug("noFFmpeg is set to " + noFFmpeg);
                }
            }

            if(lastReturnCode==1){
                mHandler.obtainMessage(MSG_RTSP, videoPath + " does NOT exist!").sendToTarget();
            } else {
                mHandler.obtainMessage(MSG_RTSP, "Finished pulling stream!").sendToTarget();
            }
        }

        public void stop(){
            FFmpeg.cancel();
            mHandler.obtainMessage(MSG_RTSP,"Stop pulling stream!").sendToTarget();
        }
    }

    private class AsyncShowPicTask extends AsyncTask<String, Void, Integer> {

        @Override
        protected Integer doInBackground(String... params) {
            Integer result = 0;
            try {
                parseResult(params[0]);
                result = 1;
            } catch (Exception e) {
                logger.info(e.toString());
            }
            return result;
        }

        @Override
        protected void onPostExecute(Integer result) {
            // Download complete. Let us update UI
            if (result == 1) {
                mGridAdapter.setGridData(mGridData);
            } else {
                Toast.makeText(GDetectionActivity.this, "Failed to fetch data!", Toast.LENGTH_SHORT).show();
            }
            mProgressBar.setVisibility(View.GONE);
        }
    }

    private void changeProgressDialogMessage(final ProgressDialog pd, final String msg) {
        Runnable changeMessage = new Runnable() {
            @Override
            public void run() {
                pd.setMessage(msg);
            }
        };
        runOnUiThread(changeMessage);
    }

    private void parseResult(String PicURL) {
        try {
            File directory = new File(PicURL);
            File[] files = directory.listFiles();
            Arrays.sort(files);
            GridItem item;

            if(files!=null) {
                for (int i = mGridData.size(); i < files.length; i++) {
                    String title = files[i].getName();
                    item = new GridItem();
                    item.setTitle(title);
                    item.setImage(PicURL + title);
                    mGridData.add(0,item);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        mHandler.obtainMessage(MSG_NUM_OF_FACES, mGridData.size() + " faces detected! ").sendToTarget();
    }

    public String getCameraIP() {
        // Get all IP Addresses
        ArrayList<String> clientList = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("/proc/net/arp"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] clientInfo = line.split(" +");
                String mac = clientInfo[3];
                if (mac.matches("..:..:..:..:..:..")) { // To make sure its not the title
                    clientList.add(clientInfo[0]);
                }
            }
        } catch (java.io.IOException aE) {
            aE.printStackTrace();
            return null;
        }

        // Get IP addresses such as 192.168.43.x, which are assigned by WiFi hotspot of the phone
        ArrayList<String> rtspList = new ArrayList<>();
        for (String client:clientList){
            if (client.replaceAll("\\.\\d+$", "").equals("192.168.43"))
                rtspList.add(client);
        }

        // Get IP addresses of the camera, which runs a RTSP server at port 554 once it starts
        String rtsp = null;
        for(String candidatRtsp : rtspList) {
            if(isReachable(candidatRtsp,554,500)){
                rtsp = candidatRtsp;
                break;
            }
        }

        return rtsp;
    }

    public boolean isReachable(String host, int port, int timeout) {
        try {
            Socket socket = new Socket();
            SocketAddress socketAddress = new InetSocketAddress(host, port);
            socket.connect(socketAddress, timeout);
            socket.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void saveImages(){
        String destinationFolder = STORAGE_PIC_URL;
        File destination = new File(destinationFolder);

        File directory = new File(PIC_URL);
        File[] picFiles = directory.listFiles();
        if(picFiles!=null) {
            for(File source:picFiles){
                try
                {
                    FileUtils.moveFileToDirectory(source, destination,true);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        mGridData.clear();
    }

    public String isValidRTSPURL(String rtspURL){
        String result;

        String temp = "rtsp://";
        int locOfRtsp = rtspURL.indexOf(temp);
        // return false if rtsp:// is not accompanied with the URL
        if (locOfRtsp == -1){
            result = "rtsp:// is not in the URL";
            logger.error(result);
            return result;
        }

        // Obtain a string excluding the "rtsp://"
        String parsedURL = rtspURL.substring(locOfRtsp + temp.length());
        // Extract IP with port if possible, also make sure that there is a path.
        int indexOfSlash = parsedURL.indexOf("/");
        String hostnameTemp;
        if (indexOfSlash != -1) {
            hostnameTemp = parsedURL.substring(0, indexOfSlash);
            // Check if there is a path following slash
            if (indexOfSlash + 1 > parsedURL.length()) {
                result =  "There is no path following the slash";
                logger.error(result);
                return result;
            }
        } else {
            result = "There is no slash in the URL";
            logger.error(result);
            return result;
        }

        // Check to see if there is a port specified. If there isn't, assume default part
        String hostName;
        int serverPort;
        int indexOfColon = hostnameTemp.indexOf(':');
        if (indexOfColon != -1) {
            // Get the IP
            hostName = hostnameTemp.substring(0, indexOfColon);
            // Get the port number
            try {
                serverPort = Integer.parseInt(hostnameTemp.substring(hostnameTemp.indexOf(':') + 1));
            } catch (NumberFormatException nfe) {
                result = "The port num is not an integer";
                logger.error(result);
                return result;
            }
        } else {
            hostName = hostnameTemp;
            serverPort = 554;
        }

        try {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Boolean isConnecable = executorService.submit(new ConnectToRTSPServer(hostName, serverPort)).get();
            executorService.shutdownNow();
            if(!isConnecable){
                result = "Unable to connect to the URL!";
                logger.info(result);
                return result;
            }
        } catch (Exception e){
            result = "Exception: Unable to connect to the URL!";
            logger.error(result);
            e.printStackTrace();
            return result;
        }

        // valid RTSP URL
        result = "valid";
        return result;
    }

    class ConnectToRTSPServer implements Callable<Boolean> {
        String hostName;
        int serverPort;
        Boolean isConnectable;

        ConnectToRTSPServer(String hostName, int serverPort){
            this.hostName = hostName;
            this.serverPort = serverPort;
        }

        public Boolean call(){
            // try to check if there is an RTSP server running
            Socket RTSPSocket = null;
            try {
                InetAddress ServerIPAddr = InetAddress.getByName(hostName);
                RTSPSocket = new Socket(ServerIPAddr, serverPort);
                isConnectable = true;
            } catch (UnknownHostException e) {
                logger.error("Could not find host ... ");
                e.printStackTrace();
                isConnectable = false;
            } catch (IOException e) {
                logger.error("Could not establish socket ... ");
                e.printStackTrace();
                isConnectable = false;
            } finally {
                // close the socket
                if(RTSPSocket!=null && RTSPSocket.isConnected()){
                    try {
                        RTSPSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return isConnectable;
        }
    }
}


