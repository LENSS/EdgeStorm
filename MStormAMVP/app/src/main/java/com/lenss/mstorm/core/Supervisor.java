package com.lenss.mstorm.core;

import android.app.Notification;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.graphics.BitmapFactory;
import android.os.Binder;
import android.os.Handler;
import android.os.IBinder;
import android.widget.Toast;
import com.google.gson.Gson;
import com.lenss.mstorm.R;
import com.lenss.mstorm.communication.masternode.MasterNodeClient;
import com.lenss.mstorm.communication.masternode.Request;
import com.lenss.mstorm.utils.Intents;
import com.lenss.mstorm.zookeeper.Assignment;
import com.lenss.mstorm.zookeeper.AssignmentProcessor;
import com.lenss.mstorm.zookeeper.ZookeeperClient;

import org.apache.log4j.Logger;
import org.apache.zookeeper.client.ZooKeeperSaslClient;

public class Supervisor extends Service implements AssignmentProcessor {
    private final String TAG="Supervisor";
    Logger logger = Logger.getLogger(TAG);

    private ZookeeperClient mZKClient=null;
    private boolean isRuning =false;
    private final IBinder mBinder = new LocalBinder();
    public static MasterNodeClient masterNodeClient;
    public static Handler mHandler;
    public static String cluster_id;
    public static Assignment newAssignment;

    Intent comptuingIntent;

    // PROCESS ID
    private static int processID = 0;

    @Override
    public void onCreate() {}

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Toast.makeText(this, "Supervisor Starting", Toast.LENGTH_SHORT).show();
        processID = android.os.Process.myPid();
        // connect to MStorm master
        if(masterNodeClient==null) {
            masterNodeClient = new MasterNodeClient(MStorm.MASTER_NODE_GUID);
            masterNodeClient.connect();
        }
        // waiting for connection
        while(!masterNodeClient.isConnected()){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("==== MStorm Master is connected ====");

        // get zookeeper address
        requestZooKeeperAddrFromMaster();
        // wait for getting Zookeeper address
        while(MStorm.ZK_ADDRESS_IP==null){
            logger.info("==== Wait for getting Zookeeper address from MStorm master ====");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("==== Get Zookeeper address from MStorm master successfully ====");

        // connect to zookeeper
        if(mZKClient==null)
        {
            System.setProperty(ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY, "false");
            mZKClient = new ZookeeperClient(this, MStorm.ZK_ADDRESS_IP);
            mZKClient.connect();
        }
        // wait for zookeeper connection
        while(!mZKClient.isConnected()){
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("==== Zookeeper is connected ====");

        // join MStorm cluster
        joinCluster();

        // Start this service as foreground service
        Notification.Builder builder = new Notification.Builder (this.getApplicationContext());
        Intent nfIntent = new Intent(this, MStorm.class);
        builder.setContentIntent(PendingIntent.getActivity(this, 0, nfIntent, 0))
                .setLargeIcon(BitmapFactory.decodeResource(this.getResources(), R.mipmap.ic_large))
                .setContentTitle("Supervisor Is Running")
                .setSmallIcon(R.mipmap.ic_launcher)
                .setContentText("Supervisor Is Running")
                .setWhen(System.currentTimeMillis());
        Notification notification = builder.build();
        notification.defaults = Notification.DEFAULT_SOUND;
        startForeground(102, notification);

        return START_NOT_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return mBinder;
    }

    @Override
    public void onDestroy() {
        Toast.makeText(this, "Supervisor is Shutting down", Toast.LENGTH_SHORT).show();
        // unregister in zookeeper
        if(mZKClient!=null)
        {
            System.setProperty(ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY, "false");
            if(cluster_id!=null){
                unregister(cluster_id);
            }
            mZKClient.stopZookeeperClient();
            mZKClient=null;
            if(Supervisor.mHandler!=null) {
                Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, "Release the connection to Zookeeper finally!").sendToTarget();
            }
        }
        // release network resource to Nimbus
        if (masterNodeClient!=null) {
            masterNodeClient.close();
            masterNodeClient = null;
            if(Supervisor.mHandler!=null) {
                Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, "Release the connection to MStorm master finally!").sendToTarget();
            }
        }

        // stop computing resource
        stopComputing();

        stopForeground(true);
        super.onDestroy();
    }

    public void setHandler(Handler handler)
    {
        mHandler = handler;
    }

    public void joinCluster() {
        Request req=new Request();
        req.setReqType(Request.JOIN);
        req.setIP(MStorm.getLocalAddress());
        masterNodeClient.sendRequest(req);
    }

    public void requestZooKeeperAddrFromMaster(){
        Request req=new Request();
        req.setReqType(Request.GETZOOKEEPER);
        req.setIP(MStorm.getLocalAddress());
        masterNodeClient.sendRequest(req);
    }

    public void register(String cluster_id){
        this.cluster_id=cluster_id;
        mZKClient.register(cluster_id);
    }

    public void unregister(String cluster_id){
        mZKClient.unregister(cluster_id);
    }

    @Override
    public void listenOnTaskAssignment(String cluster_id){
        mZKClient.listenOnTaskAssignment(cluster_id);
    }

    @Override
    public void startComputing(String assignment){
        newAssignment=new Gson().fromJson(assignment, Assignment.class);

        ////// can be commented out for real exercise !!!!!!!!!!!!!!!!!!!!!
//        // If the apk file is not at the client, get it from server
//        if(!FileClientHandler.FileOnMachine) {
//            // request apk file from Nimbus first
//            Request req = new Request();
//            req.setReqType(Request.GETAPKFILE);
//            req.setIP(MStorm.getLocalAddress());
//            req.setFileName(newAssignment.getApk());
//            masterNodeClient.sendRequest(req);
//
//            // Wait apk file to be downloaded
//            while (!FileClientHandler.FileOnMachine) {
//                try {
//                    logger.info("File not download yet!");
//                    Thread.sleep(1000);
//                } catch (InterruptedException e1) {
//                    e1.printStackTrace();
//                }
//            }
//        }
        ////// can be commented out for real exercise !!!!!!!!!!!!!!!!!!!!!

        if(!isRuning) { // the computing service is not running, start it
            if (newAssignment.getAssginedNodes().contains(MStorm.GUID)) {
                Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, "New Assignment, start computing!").sendToTarget();
                comptuingIntent = new Intent(Intents.ACTION_START_COMPUTING_NODE);
                comptuingIntent.putExtra(ComputingNode.ASSIGNMENT, assignment);
                startService(Intents.createExplicitFromImplicitIntent(this, comptuingIntent));
                isRuning = true;
            }
        } else { // the computing service is running, stop the old and start the new
            if (newAssignment.getPreAssignedNodes().contains(MStorm.GUID)) {
                ComputingNode.setPauseOrContinue(true) ;  // pause more tuples into MStorm

              // If there is a component heavily congested, this will not work
                int noMoreTuplesFlag = 0;

        /*      while(noMoreTuplesFlag<10){     // Triple check to ensure that there is no more tuples
                    if(ComputingNode.noMoreTupleInMStorm())
                        noMoreTuplesFlag++;
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/

                try {
                    logger.info("Wait the remaining tuples to be processed!");
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // stop previous computing service
                stopComputing();

                // start the new computing service
                Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"Start computing again!").sendToTarget();
                comptuingIntent=new Intent(Intents.ACTION_START_COMPUTING_NODE);
                comptuingIntent.putExtra(ComputingNode.ASSIGNMENT, assignment);
                startService(Intents.createExplicitFromImplicitIntent(this, comptuingIntent));
                isRuning = true;
            }
        }
    }

    @Override
    public void stopComputing() {
        if(isRuning) {
            Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"Stop computing!").sendToTarget();
            stopService(Intents.createExplicitFromImplicitIntent(this, new Intent(Intents.ACTION_STOP_COMPUTING_NODE)));
            isRuning=false;
            newAssignment = null;
        }
    }

    @Override
    public void stopComputing(String assignment){
        Assignment cancelAssign=new Gson().fromJson(assignment, Assignment.class);
        if(cancelAssign.getAssginedNodes().contains(MStorm.GUID)){
            stopComputing();
        }
    }

    public static int getProcessID(){
        return processID;
    }

    public class LocalBinder extends Binder {
        Supervisor getService() {
            // Return this instance of LocalService so clients can call public methods
            return Supervisor.this;
        }
    }

}
