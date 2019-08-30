package com.lenss.cmy;

import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Message;
import android.support.v7.app.ActionBarActivity;
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
import com.lenss.cmy.grecognition.GRecognizerTopology;
import com.arthenica.mobileffmpeg.FFmpeg;
import com.lenss.mstorm.communication.masternode.Reply;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.topology.StormSubmitter;
import com.tzutalin.dlib.Constants;
import com.tzutalin.dlib.FaceRec;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import static java.lang.Thread.sleep;

public class GRecognitionActivity extends ActionBarActivity {
    public static final String TAG = "GRecognitionActivity";

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
    private static final String apkFileDirectory = MStormDir + "APK/";
    private static final String apkFileName = "GRecognition.apk";

    private static final String CAMERA_SOURCE = "0";
    private static final String VIDEO_SOURCE = "1";

    private static final int NUM_OF_FACES=0;
    private TextView result;

    // initial parallelism of face detector and recognizer component
    public static int myDistributorParallel = 1;
    public static int faceDetectorParallel = 1;
    public static int faceRecognizerParallel = 1;
    public static int faceSaverParallel = 1;

    // initial stream grouping method of each component
    public static int faceDetectorGroupingMethod = Topology.Shuffle;
    public static int faceRecognizerGroupingMethod = Topology.Shuffle;
    public static int faceSaverGroupingMethod = Topology.Shuffle;

    // schedule requirements of each component
    public static int myDistributorScheduleReq = Topology.Schedule_Local;
    public static int faceDetectorScheduleReq = Topology.Schedule_Any;
    public static int faceRecognizerScheduleReq = Topology.Schedule_Any;
    public static int faceSaverScheduleReq = Topology.Schedule_Local;

    // Input frame rate
    public static String frameRate = "1";

    public static ReadFromCamera rfc = null;
    public static ReadFromVideo rfv = null;
    public static String streamSource = CAMERA_SOURCE;

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

                Intent intent = new Intent(GRecognitionActivity.this, DetailsActivity.class);

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
                handler.postDelayed(this, 10);
            }
        }, 10);
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

    @Override
    protected void onDestroy() {
        logger.info("onDestroy");
        stopPullStream();
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
        if(id == R.id.action_train){
            new TrainFaces().execute();
        } else if(id == R.id.action_set_parallelTasks) {
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);

            final EditText fdBox = new EditText(this);
            fdBox.setHint("faceDetector:"+faceDetectorParallel);
            layout.addView(fdBox);

            final EditText frBox = new EditText(this);
            frBox.setHint("faceRecognizer:"+faceRecognizerParallel);
            layout.addView(frBox);

            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    faceDetectorParallel = Integer.parseInt(fdBox.getText().toString());
                    faceRecognizerParallel =  Integer.parseInt(frBox.getText().toString());
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        }
//        else if (id == R.id.action_set_groupingMethod) {
//            LinearLayout layout = new LinearLayout(this);
//            layout.setOrientation(LinearLayout.VERTICAL);
//            AlertDialog.Builder alert = new AlertDialog.Builder(this);
//            final EditText distributorBox = new EditText(this);
//            distributorBox.setHint("distributor (shuffle:1, feedbackBased:2):" + distributorGroupingMethod);
//            layout.addView(distributorBox);
//            alert.setView(layout);
//            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
//                public void onClick(DialogInterface dialog, int which) {
//                    // continue with delete
//                    distributorGroupingMethod = Integer.parseInt(distributorBox.getText().toString());
//                }
//            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
//                public void onClick(DialogInterface dialog, int which) {
//                    // do nothing
//                }
//            }).setIcon(android.R.drawable.ic_dialog_alert).show();
//        }
        else if(id == R.id.action_set_streamSource){
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);
            final EditText ssBox = new EditText(this);
            ssBox.setHint("Camera:0 | Video: 1");
            layout.addView(ssBox);
            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    streamSource = ssBox.getText().toString();
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else if(id == R.id.action_set_frameRate){
            LinearLayout layout = new LinearLayout(this);
            layout.setOrientation(LinearLayout.VERTICAL);
            AlertDialog.Builder alert = new AlertDialog.Builder(this);
            final EditText frBox = new EditText(this);
            frBox.setHint("Default (F/s): " + frameRate);
            layout.addView(frBox);
            alert.setView(layout);
            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // continue with delete
                    frameRate = frBox.getText().toString();
                }
            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int which) {
                    // do nothing
                }
            }).setIcon(android.R.drawable.ic_dialog_alert).show();
        } else {
            Topology topology = GRecognizerTopology.createTopology();
            StormSubmitter submitter = new StormSubmitter(this, apkFileDirectory);
            submitter.submitTopology(apkFileName,topology);
            // wait for reply containing topologyID
            Reply reply;
            while((reply=submitter.getReply())==null){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            int topologyID = new Integer(reply.getContent());
            if(topologyID != 0)
                Toast.makeText(this, "Topology Scheduled!", Toast.LENGTH_SHORT).show();
        }
        return super.onOptionsItemSelected(item);
    }

    public void clickToggleRecording(@SuppressWarnings("unused") View unused) {
        mRecordingEnabled = !mRecordingEnabled;
        updateControls();

        if(mRecordingEnabled)
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
        if(streamSource.equals(CAMERA_SOURCE)){ // pull stream from camera
            if(rfc==null){
                rfc = new ReadFromCamera();
                new Thread(rfc).start();
            }
        } else { // pull stream from video
            if (rfv == null) {
                rfv = new ReadFromVideo();
                new Thread(rfv).start();
            }
        }

//         pull stream to FD App
//        if(rfs==null){
//            rfs = new ReadFromStream();
//            new Thread(rfs).start();
//        }
    }

    public void stopPullStream(){
        if(streamSource.equals(CAMERA_SOURCE)) { // pull stream from camera
            if (rfc != null) {
                rfc.stop();
                rfc = null;
            }
        } else { // stop pulling from video
            if (rfv != null) {
                rfv.stop();
                rfv = null;
            }
        }
    }

    private final Handler mHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            result.setText(msg.obj.toString());
        }
    };

    class ReadFromCamera implements Runnable{
        public void run(){
            String rtsp;

            while ((rtsp = getCameraIP()) == null ) {   // camera is not open yet
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            int lastReturnCode = 1;
            // 1:AmbaRTSPServer@YICamera restarts, because the camera has not started recording and there is no stream to be pull to the phone.
            while ((lastReturnCode == 1)) {
                try {
                    Thread.sleep(1000);     // wait for RTSP server restarting at the camera
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
                lastReturnCode = FFmpeg.execute("-i rtsp://" + rtsp + "/live -qscale:v 3 -r " + frameRate + " " + RAW_PIC_URL + "/sample%d.jpg");
            }
            // 0: FFmpeg client to AmbaRTSPServer@YICamera is timeout, because the camera stops recording.
            if(lastReturnCode == 0){
                stopPullStream();
                startPullStream();
            }
        }

        public void stop(){
            FFmpeg.cancel();
        }
    }

    class ReadFromVideo implements Runnable{
        String videoPath = MStormDir + "Videos/test.mp4";
        public void run(){
            FFmpeg.execute("-i " +  videoPath + " -qscale:v 3 -r " + frameRate + " " + RAW_PIC_URL + "/sample%d.jpg");
        }
        public void stop(){
            FFmpeg.cancel();
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
                Toast.makeText(GRecognitionActivity.this, "Failed to fetch data!", Toast.LENGTH_SHORT).show();
            }
            mProgressBar.setVisibility(View.GONE);
        }
    }

    private class TrainFaces extends AsyncTask<Void, Void, Void> {
        ProgressDialog dialog = new ProgressDialog(GRecognitionActivity.this);

        @Override
        protected void onPreExecute() {
            Log.d(TAG, "initRecAsync onPreExecute called");
            dialog.setMessage("Initializing...");
            dialog.setCancelable(false);
            dialog.show();
            super.onPreExecute();
        }

        @Override
        protected Void doInBackground(Void... args) {
            // create dlib_rec_example directory in sd card and copy model files
            File folder = new File(Constants.getDLibDirectoryPath());
            boolean success = false;
            if (!folder.exists()) {
                success = folder.mkdirs();
            }
            if (success) {
                File image_folder = new File(Constants.getDLibImageDirectoryPath());
                image_folder.mkdirs();
                if (!new File(Constants.getFaceShapeModelPath()).exists()) {
                    Utils.copyFileFromRawToOthers(GRecognitionActivity.this, R.raw.shape_predictor_5_face_landmarks, Constants.getFaceShapeModelPath());
                }
                if (!new File(Constants.getFaceDescriptorModelPath()).exists()) {
                    Utils.copyFileFromRawToOthers(GRecognitionActivity.this, R.raw.dlib_face_recognition_resnet_model_v1, Constants.getFaceDescriptorModelPath());
                }
            } else {
                //Log.d(TAG, "error in setting dlib_rec_example directory");
            }

            FaceRec mFaceRec = new FaceRec(Constants.getDLibDirectoryPath());
            changeProgressDialogMessage(dialog, "Training faces ...");
            mFaceRec.train();
            return null;
        }

        @Override
        protected void onPostExecute(Void result) {
            if(dialog != null && dialog.isShowing()){
                dialog.dismiss();
            }
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
            GridItem item;

            if(files!=null) {
                for (int i = mGridData.size(); i < files.length; i++) {
                    String title = files[i].getName();
                    item = new GridItem();
                    item.setTitle(title);
                    item.setImage(PicURL + title);
                    mGridData.add(item);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        mHandler.obtainMessage(NUM_OF_FACES, mGridData.size() + " faces detected! ").sendToTarget();
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
}

