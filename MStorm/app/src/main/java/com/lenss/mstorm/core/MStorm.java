package com.lenss.mstorm.core;

import android.Manifest;
import android.app.AlertDialog;
import android.content.ComponentName;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.graphics.Bitmap;
import android.net.wifi.WifiManager;
import android.os.Environment;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.text.format.Formatter;
import android.text.format.Time;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.lenss.mstorm.R;
import com.lenss.mstorm.communication.internodes.StreamSelector;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.GPSTracker;
import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.utils.Intents;
import com.lenss.mstorm.utils.TAGs;

import org.apache.log4j.Logger;
import java.io.IOException;

import edu.tamu.cse.lenss.edgeKeeper.client.EKClient;

public class MStorm extends ActionBarActivity{
    // For Logs
    public static final String TAG = "MStorm";
    public static final String MStormDir = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/";
    public static final String LOG_URL = MStormDir + "mstorm.log";
    public static final String ASSIGNMENT_URL = MStormDir + "serAssign.txt";
    public static final String apkFileDirectory = MStormDir + "APK/";
    Logger logger;

//    // Permissions
//    private static final int WAKELOCK_PERMISSION_CODE = 100;
//    private static final int STORAGE_PERMISSION_CODE = 101;
//    private static final int NETWORK_PERMISSION_CODE = 102;
//    private static final int LOCATION_PERMISSION_CODE = 103;

    // Service Flag
    private boolean SERVICE_STARTED = false;
    private boolean mBound = false;

    // TextView
    private TextView mLog = null;
    private TextView mDataRecvd = null;
    private TextView mDataSend = null;
    private TextView mFaceDetected = null;
    private TextView mPeriodReport = null;

    //Message types
    public static final int Message_LOG = 0;
    public static final int Message_GOP_RECVD = 1;
    public static final int Message_GOP_SEND = 2;
    public static final int Message_PERIOD_REPORT = 3;
    public static final int CLUSTER_ID = 4;

    //Zookeeper
    public static final int SESSION_TIMEOUT = 10000;
    public static String ZK_ADDRESS_IP = null;

    //Master Node
    public static String MASTER_NODE_GUID = null;
    public static String MASTER_NODE_IP = null;
    public static final int MASTER_PORT = 12016;

    // Own Address
    public static String GUID;
    private static String localAddress;

    // Public or Private
    public static String isPublicOrPrivate;

    /*Added by Amran*/
    // Public or Private
    public static String isMobileOrServer;

    // Availability of this node
    public static double availability = 1.0;

    // Stream selection strategy
    public static int streamSelectionStrategy = StreamSelector.NO_SELECTION;
    public static int maxInQueueLength = 10;
    public static double startDroppingThreshold = 0.5;

    // GPS
    public static GPSTracker gps = null;

    // context
    private static Context context = null;

    //Supervisor Service
    private Supervisor mSupervisor;

    // Connection to Supervisor service
    private ServiceConnection mConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName componentName, IBinder iBinder) {
            mSupervisor = ((Supervisor.LocalBinder) iBinder).getService();
            mSupervisor.setHandler(mHandler);
            mBound = true;
        }

        @Override
        public void onServiceDisconnected(ComponentName componentName) {
            mSupervisor = null;
            mBound = false;
        }
    };

    public static String getLocalAddress() {
        return localAddress;
    }

    public static String getZookeeperAddress(){
        return ZK_ADDRESS_IP;
    }

    public static Context getContext(){
        return context;
    }

    private boolean isReadyToStartService(){
        MASTER_NODE_GUID = GNSServiceHelper.getMasterNodeGUID();
        MASTER_NODE_IP = GNSServiceHelper.getIPInUseByGUID(MASTER_NODE_GUID);
        if(MASTER_NODE_GUID==null || MASTER_NODE_IP == null)
            return false;
        else
            return true;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_greporter);

//        checkPermissions(new String[]{Manifest.permission.WAKE_LOCK}, WAKELOCK_PERMISSION_CODE);
//        checkPermissions(new String[]{Manifest.permission.INTERNET, Manifest.permission.ACCESS_NETWORK_STATE, Manifest.permission.ACCESS_WIFI_STATE}, NETWORK_PERMISSION_CODE);
//        checkPermissions(new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE, Manifest.permission.READ_EXTERNAL_STORAGE}, STORAGE_PERMISSION_CODE);
//        checkPermissions(new String[]{Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION}, WAKELOCK_PERMISSION_CODE);

        mLog = (TextView) findViewById(R.id.log);
        mFaceDetected = (TextView) findViewById(R.id.facedetected);
        mDataSend = (TextView) findViewById(R.id.senddata);
        mDataRecvd = (TextView) findViewById(R.id.datarcvd);
        mPeriodReport = (TextView) findViewById(R.id.periodReport);
        gps = new GPSTracker(MStorm.this);
        context = getApplicationContext();

        mLog.append("\n======================================");

        // Configure the logger to store logs in file
        try {
            TAGs.initLogger(LOG_URL);
            logger = Logger.getLogger(TAG);
        } catch (IOException e) {
            mLog.append("Can not create log file probably due to insufficient permission");
        }

        /// Get own GUID
        GUID = GNSServiceHelper.getOwnGUID();
        if(GUID == null) {
            mLog.append("\nO_GUID: Cannot get, check if EdgeKeeper starts!");
            onStop();
        } else {
            mLog.append("\nO_GUID: " + GUID);
        }

        /// For Real Machines WiFi
/*        WifiManager wm = (WifiManager) getApplicationContext().getSystemService(WIFI_SERVICE);
        localAddress = Formatter.formatIpAddress(wm.getConnectionInfo().getIpAddress());*/

        /// For Real Machines LTE
        localAddress = Helper.getIPAddress(true);

        /// For virtual Machines
        //localAddress = Helper.getIPAddress(true);

        /// For containers
        //localAddress = Helper.getIPAddress();

        /// Get Master Node GUID and IP
        MASTER_NODE_GUID = GNSServiceHelper.getMasterNodeGUID();
        if (MASTER_NODE_GUID == null){
            mLog.append("\nM_GUID: Cannot get, check EdgeKeeper/MStormMaster status!");
            onStop();
        } else {
            mLog.append("\nM_GUID: "+ MASTER_NODE_GUID);
        }

        MASTER_NODE_IP = GNSServiceHelper.getIPInUseByGUID(MASTER_NODE_GUID);
        if (MASTER_NODE_IP == null){
            mLog.append("\nM_IP: Cannot get, check EdgeKeeper/MStormMaster status!");
            onStop();
        } else {
            mLog.append("\nM_IP: "+ MASTER_NODE_IP);
        }

        if (MASTER_NODE_GUID!=null){
            mLog.append("\nChoose \"Start\" on the up-right corner to join a cluster ... ");
        }

        mLog.append("\n======================================");

//        /// Get Zookeeper IP
//        ZK_ADDRESS_IP = GNSServiceHelper.getZookeeperIP();

        /// Get own address status
        isPublicOrPrivate = "0";    // This can be get from some configuration file later: 0 means private, 1 means public
        /*Added by Amran*/
        isMobileOrServer = "0";    // This can be obtained from some configuration file later: 0 means mobile device, 1 means edge server
    }

    @Override
    protected void onStart() {
        super.onStart();
        if (!mBound) {
            Intent intent = new Intent(this, Supervisor.class);
            bindService(intent, mConnection, Context.BIND_AUTO_CREATE);
            mBound = true;
        }
    }

    @Override
    protected void onStop() {
        logger.info("onStop");
        super.onStop();
    }

    @Override
    public void onDestroy() {
        logger.info("onDestroy");
        if (SERVICE_STARTED) {
            SERVICE_STARTED = false;
            if (mBound) {
                unbindService(mConnection);
                mBound = false;
            }
            stopService(Intents.createExplicitFromImplicitIntent(this, new Intent(Intents.ACTION_STOP_SUPERVISOR)));

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        super.onDestroy();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_greporter, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();
//        if(id == R.id.action_set_availability){
//            LinearLayout layout = new LinearLayout(this);
//            layout.setOrientation(LinearLayout.VERTICAL);
//            AlertDialog.Builder alert = new AlertDialog.Builder(this);
//
//            final EditText availBox = new EditText(this);
//            availBox.setHint("Availability:" + availability);
//            layout.addView(availBox);
//
//            alert.setView(layout);
//            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
//                public void onClick(DialogInterface dialog, int which) {
//                    availability = Double.parseDouble(availBox.getText().toString());
//                }
//            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
//                public void onClick(DialogInterface dialog, int which) {
//                    // do nothing
//                }
//            }).setIcon(android.R.drawable.ic_dialog_alert).show();
//        } else if(id == R.id.action_set_in_queue_parameter){
//            LinearLayout layout = new LinearLayout(this);
//            layout.setOrientation(LinearLayout.VERTICAL);
//            AlertDialog.Builder alert = new AlertDialog.Builder(this);
//
//            final EditText inQueueLengthBox = new EditText(this);
//            inQueueLengthBox.setHint("MaxInQueueLength:"+maxInQueueLength);
//            layout.addView(inQueueLengthBox);
//
//            final EditText startDroppingBox = new EditText(this);
//            startDroppingBox.setHint("StartDroppingThreshold:"+startDroppingThreshold);
//            layout.addView(startDroppingBox);
//
//            alert.setView(layout);
//            alert.setPositiveButton("Confirm", new DialogInterface.OnClickListener() {
//                public void onClick(DialogInterface dialog, int which) {
//                    // continue with delete
//                    maxInQueueLength = Integer.parseInt(inQueueLengthBox.getText().toString());
//                    StreamSelector.setMaxQueueLength(maxInQueueLength);
//                    startDroppingThreshold =  Double.parseDouble(startDroppingBox.getText().toString());
//                    StreamSelector.setStartDroppingThreshold(startDroppingThreshold);
//                }
//            }).setNegativeButton(android.R.string.no, new DialogInterface.OnClickListener() {
//                public void onClick(DialogInterface dialog, int which) {
//                    // do nothing
//                }
//            }).setIcon(android.R.drawable.ic_dialog_alert).show();
//        } else if(id == R.id.action_set_stream_selection_strategy){
//            LinearLayout layout = new LinearLayout(this);
//            layout.setOrientation(LinearLayout.VERTICAL);
//            CharSequence[] streamSelectionStrategies = {"NoSelection", "QueueThresholdBased", "QueueProbBased", "IOSpeedBased", "IOSpeedProbBased"};
//            new AlertDialog.Builder(this)
//                    .setSingleChoiceItems(streamSelectionStrategies, 0, null)
//                    .setPositiveButton("OK", new DialogInterface.OnClickListener() {
//                        public void onClick(DialogInterface dialog, int whichButton) {
//                            dialog.dismiss();
//                            int selectedPosition = ((AlertDialog)dialog).getListView().getCheckedItemPosition();
//                            switch (selectedPosition) {
//                                case 0: // No Selection
//                                    streamSelectionStrategy = StreamSelector.NO_SELECTION;
//                                    break;
//                                case 1: // Threshold Based
//                                    streamSelectionStrategy = StreamSelector.QUEUE_THRESHOLD_BASED;
//                                    break;
//                                case 2: // Probability Based
//                                    streamSelectionStrategy = StreamSelector.QUEUE_PROBABILITY_BASED;
//                                    break;
//                                case 3: // IOSpeed Based
//                                    streamSelectionStrategy = StreamSelector.IO_SPEED_BASED;
//                                    break;
//                                case 4: // IOSpeed Probability Based
//                                    streamSelectionStrategy = StreamSelector.IO_SPEED_PROBABILITY_BASED;
//                                    break;
//                                default:
//                                    break;
//                            }
//                            StreamSelector.setSelectStrategy(streamSelectionStrategy);
//                        }
//                    }).show();
//        } else if (id == R.id.action_start_computing_service) {
//            if (SERVICE_STARTED == false) {
//                SERVICE_STARTED = true;
//                if (!mBound) {
//                    Intent intent = new Intent(this, Supervisor.class);
//                    bindService(intent, mConnection, Context.BIND_AUTO_CREATE);
//                    mBound = true;
//                }
//                // Start supervisor
//                startService(Intents.createExplicitFromImplicitIntent(this, new Intent(Intents.ACTION_START_SUPERVISOR)));
//            } else {
//                Toast.makeText(this, "Supervisor Already ON", Toast.LENGTH_SHORT).show();
//            }
//        } else {
//            if (SERVICE_STARTED) {
//                SERVICE_STARTED = false;
//                if (mBound) {
//                    unbindService(mConnection);
//                    mBound = false;
//                }
//                mLog.setText("");
//                mFaceDetected.setText("");
//                mDataSend.setText("");
//                mDataRecvd.setText("");
//                mPeriodReport.setText("");
//                stopService(Intents.createExplicitFromImplicitIntent(this, new Intent(Intents.ACTION_STOP_SUPERVISOR)));
//            } else {
//                Toast.makeText(this, "Supervisor Already OFF", Toast.LENGTH_SHORT).show();
//            }
//        }

        if (id == R.id.action_start_computing_service) {
            if(!isReadyToStartService()){
                Toast.makeText(this, "Cannot start! Check EdgeKeeper and MStorm master status!", Toast.LENGTH_SHORT).show();
                return super.onOptionsItemSelected(item);
            }

            if (SERVICE_STARTED == false) {
                SERVICE_STARTED = true;
                if (!mBound) {
                    Intent intent = new Intent(this, Supervisor.class);
                    bindService(intent, mConnection, Context.BIND_AUTO_CREATE);
                    mBound = true;
                }
                // Start supervisor
                startService(Intents.createExplicitFromImplicitIntent(this, new Intent(Intents.ACTION_START_SUPERVISOR)));
            } else {
                Toast.makeText(this, "Supervisor Already ON!", Toast.LENGTH_SHORT).show();
            }
        } else {
            if (SERVICE_STARTED) {
                SERVICE_STARTED = false;
                if (mBound) {
                    unbindService(mConnection);
                    mBound = false;
                }
                mLog.setText("");
                mFaceDetected.setText("");
                mDataSend.setText("");
                mDataRecvd.setText("");
                mPeriodReport.setText("");
                stopService(Intents.createExplicitFromImplicitIntent(this, new Intent(Intents.ACTION_STOP_SUPERVISOR)));
            } else {
                Toast.makeText(this, "Supervisor Already OFF", Toast.LENGTH_SHORT).show();
            }
        }

        return super.onOptionsItemSelected(item);
    }

    private final Handler mHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            switch (msg.what) {
                case Message_LOG:
                    logger.info(msg.obj.toString());
                    mLog.append('\n' + msg.obj.toString());
                    break;
                case CLUSTER_ID:
                    String clusterID = msg.obj.toString();
                    logger.info("Cluster ID is " + clusterID);
                    mSupervisor.register(clusterID);
                    mSupervisor.listenOnTaskAssignment(clusterID);
                    mLog.append('\n' + "Cluster ID is " + clusterID);
                    break;
                case Message_GOP_SEND:
                    mDataSend.setText(msg.arg1 + " bytes SEND!" + '\n');
                    break;
                case Message_GOP_RECVD:
                    mDataRecvd.setText(msg.arg1 + " bytes RECEIVED!");
                    break;
                case Message_PERIOD_REPORT:
                    mPeriodReport.setText(msg.obj.toString());
                    break;
                default:
                    super.handleMessage(msg);
            }
        }
    };

//    // Function to check and request permission.
//    public void checkPermissions(String[] permissions, int requestCode)
//    {
//        for(String permission: permissions){
//            if (ContextCompat.checkSelfPermission(MStorm.this, permission) == PackageManager.PERMISSION_DENIED) {
//                // Requesting the permission
//                ActivityCompat.requestPermissions(MStorm.this, new String[] {permission}, requestCode);
//            } else {
//                Toast.makeText(MStorm.this, "Permission already granted", Toast.LENGTH_SHORT).show();
//            }
//        }
//    }

//    @Override
//    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults)
//    {
//        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
//
//        if (requestCode == WAKELOCK_PERMISSION_CODE) {
//            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
//                Toast.makeText(MStorm.this, "Wake Lock Permission Granted", Toast.LENGTH_SHORT).show();
//            }
//            else {
//                Toast.makeText(MStorm.this, "Wake Lock Permission Denied", Toast.LENGTH_SHORT).show();
//            }
//        } else if (requestCode == STORAGE_PERMISSION_CODE) {
//            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
//                Toast.makeText(MStorm.this,"Storage Permission Granted", Toast.LENGTH_SHORT).show();
//            } else {
//                Toast.makeText(MStorm.this,"Storage Permission Denied", Toast.LENGTH_SHORT).show();
//            }
//        } else if (requestCode == NETWORK_PERMISSION_CODE){
//            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
//                Toast.makeText(MStorm.this,"Network Permission Granted", Toast.LENGTH_SHORT).show();
//            } else {
//                Toast.makeText(MStorm.this,"Network Permission Denied", Toast.LENGTH_SHORT).show();
//            }
//        } else if (requestCode == LOCATION_PERMISSION_CODE){
//            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
//                Toast.makeText(MStorm.this,"Location Permission Granted", Toast.LENGTH_SHORT).show();
//            } else {
//                Toast.makeText(MStorm.this,"Location Permission Denied", Toast.LENGTH_SHORT).show();
//            }
//        }
//    }
}
