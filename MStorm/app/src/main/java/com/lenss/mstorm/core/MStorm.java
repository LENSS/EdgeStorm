package com.lenss.mstorm.core;

import android.app.AlertDialog;
import android.content.ComponentName;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.ServiceConnection;
import android.graphics.Bitmap;
import android.net.wifi.WifiManager;
import android.os.Environment;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
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
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.GPSTracker;
import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.utils.Intents;
import com.lenss.mstorm.utils.TAGs;

import org.apache.log4j.Logger;
import java.io.IOException;

public class MStorm extends ActionBarActivity {
    // For Logs
    public static final String TAG = "MStorm";
    public static final String MStormDir = Environment.getExternalStorageDirectory().getPath() + "/distressnet/MStorm/";
    public static final String LOG_URL = MStormDir + "mstorm.log";
    public static final String ASSIGNMENT_URL = MStormDir + "serAssign.txt";
    public static final String apkFileDirectory = MStormDir + "APK/";

    Logger logger;

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
    public static String ZK_ADDRESS_IP;

    //Master Node
    public static String MASTER_NODE_GUID;
    public static String MASTER_NODE_IP;
    public static final int MASTER_PORT = 12016;

    // Own Address
    public static String GUID;
    private static String localAddress;

    // Public or Private
    public static String isPublicOrPrivate;

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

    public static Context getContext(){
        return context;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_greporter);
        mLog = (TextView) findViewById(R.id.log);
        mFaceDetected = (TextView) findViewById(R.id.facedetected);
        mDataSend = (TextView) findViewById(R.id.senddata);
        mDataRecvd = (TextView) findViewById(R.id.datarcvd);
        mPeriodReport = (TextView) findViewById(R.id.repriodReport);

        gps = new GPSTracker(MStorm.this);
        context = getApplicationContext();

        // Configure the logger to store logs in file
        try {
            TAGs.initLogger(LOG_URL);
            logger = Logger.getLogger(TAG);
        } catch (IOException e) {
            mLog.append("Can not create log file probably due to insufficient permission");
            logger.error(e);
        }

        /// Get own GUID
        GUID = GNSServiceHelper.getOwnGUID();
        if(GUID == null) {
            mLog.append("\nEdgeKeeper unreachable!");
            onStop();
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
            mLog.append("\nMStorm Master Unregistered!");
            onStop();
        }

        MASTER_NODE_IP = GNSServiceHelper.getIPInUseByGUID(MASTER_NODE_GUID);
        if (MASTER_NODE_IP == null){
            mLog.append("\nMStorm Master unreachable!\n");
            onStop();
        }

        /// Get Zookeeper IP
        ZK_ADDRESS_IP = GNSServiceHelper.getZookeeperIP();

        /// Get own address status
        isPublicOrPrivate = "0";    // This can be get from some configuration file later: 0 means private, 1 means public
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
        if (id == R.id.action_start_computing_service) {
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
                Toast.makeText(this, "Supervisor Already ON", Toast.LENGTH_SHORT).show();
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
}
