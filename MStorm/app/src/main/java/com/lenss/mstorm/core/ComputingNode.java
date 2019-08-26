package com.lenss.mstorm.core;

import android.app.Notification;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;
import android.os.Process;
import android.os.Binder;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.os.Message;
import android.widget.Toast;
import com.google.gson.Gson;
import com.lenss.mstorm.R;
import com.lenss.mstorm.communication.internodes.CommunicationClient;
import com.lenss.mstorm.communication.internodes.CommunicationServer;
import com.lenss.mstorm.communication.internodes.ChannelManager;
import com.lenss.mstorm.communication.internodes.Dispatcher;
import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.executor.Executor;
import com.lenss.mstorm.executor.ExecutorManager;
import com.lenss.mstorm.status.StatusReporter;
import com.lenss.mstorm.topology.BTask;
import com.lenss.mstorm.topology.Topology;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Serialization;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import dalvik.system.DexClassLoader;

import static java.lang.Thread.sleep;

public class ComputingNode extends Service {
    /// LOGGER
    private final String TAG="ComputingNode";
    Logger logger = Logger.getLogger(TAG);

    //// SOME CONSTANT STRING
    public static final String ASSIGNMENT = "NEW_ASSIGNMENT";
    public static final String SPOUT_ADDRESSES=MStorm.MStormDir+"SpoutAddrs";
    public static final String EXEREC_ADDRESSES=MStorm.MStormDir+"ExeRecord";

    //// MESSAGE TYPES
    public static final int NEW_TASKS = 0;
    public static final int SHUTDOWN_TASK = 1;
    public static final int SHUTDOWN_COMPUTING_NODE = 2;

    //// EXECUTORS
    private ExecutorManager mExecutorManager;
    private LinkedBlockingDeque<Runnable> mExecutorQueue;
    private static final int NUMBER_OF_CORE_EXECUTORS =8;
    private static final int NUMBER_OF_MAX_EXECUTORS = 16;
    private static final int KEEP_ALIVE_TIME = 60;
    private static final TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;

    // pause or continue stream processing;, 0 continue, 1 pause
    private static int pauseOrContinue = 0;

    private StatusReporter statusReporter;
    private Thread reporterThread;
    private Dispatcher dispatcher;

    // processID of computing node
    private static int processID = 0;
    // assignment from scheduler
    private static Assignment assignment;
    // assignment string from scheduler
    private String serAssignment;
    // topology string from scheduler
    private static String serTopology;
    // topology from scheduler
    private static Topology topology;

    private final IBinder mBinder = new LocalBinder();

    //// COMMUNICATION
    // sever and client on computing nodes
    private CommunicationServer mServer;
    private CommunicationClient mClient;
    private volatile int allConnected=0;

    private final class ServiceHandler extends Handler {
        public ServiceHandler(Looper looper) {
            super(looper);
        }
        @Override
        public void handleMessage(Message msg) {
            switch (msg.arg1) {
                case NEW_TASKS:
                    break;
                case SHUTDOWN_TASK:
                    break;
                case SHUTDOWN_COMPUTING_NODE:
                    break;
            }
        }
    }

    public class LocalBinder extends Binder {
        ComputingNode getService() {    // Return this instance of LocalService so clients can call public methods
            return ComputingNode.this;
        }
    }

    @Override
    public void onCreate() {
        //Initial the ExecutorManager
        mExecutorQueue = new LinkedBlockingDeque<Runnable>();
        mExecutorManager = new ExecutorManager(NUMBER_OF_CORE_EXECUTORS, NUMBER_OF_MAX_EXECUTORS, KEEP_ALIVE_TIME, KEEP_ALIVE_TIME_UNIT, mExecutorQueue);

/*        HandlerThread thread = new HandlerThread("ServiceStartArguments", Process.THREAD_PRIORITY_BACKGROUND);
        thread.start();

          // Get the HandlerThread's Looper and use it for our Handler
          mServiceLooper = thread.getLooper();
          mServiceHandler = new ServiceHandler(mServiceLooper); */
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Toast.makeText(this, "service starting", Toast.LENGTH_SHORT).show();

        System.out.println("Running in the foreground successfully\n");

        // record the processID of computing node
        processID = Process.myPid();

        pauseOrContinue = 0; // allow pulling more tuple for stream processing

        // create a status reporter
        statusReporter = StatusReporter.getInstance();
        statusReporter.initializeStatusReporter(Supervisor.mHandler, this.getApplicationContext(), Supervisor.masterNodeClient, Supervisor.cluster_id);


        // get the assignment and local tasks, assign corresponding queues for local tasks
        if(intent==null){
            logger.error("The Intent passed from supervisor is null ...");
            return START_NOT_STICKY;
        }

        serAssignment = intent.getStringExtra(ComputingNode.ASSIGNMENT);
        assignment = new Gson().fromJson(serAssignment, Assignment.class);
        serTopology = assignment.getSerTopology();
        topology = new Gson().fromJson(serTopology, Topology.class);

        // add queues for local tasks
        ArrayList<Integer> localTasks = assignment.getNode2Tasks().get(MStorm.GUID);
        if (localTasks!=null) {
            for (int i = 0; i < localTasks.size(); i++) {
                int taskId = localTasks.get(i);
                MessageQueues.addQueuesForTask(taskId);
            }
        }

//        // record spoutAddress to files
//        ArrayList<String> SpoutAddrs=assignment.getSpoutAddr();
//        String addrs = "";
//        for(String s: SpoutAddrs){
//            String addrIP = GNSServiceHelper.getIPInUseByGUID(s);
//            addrs += addrIP+"\n";
//        }
//        try {
//            FileWriter fw = new FileWriter(SPOUT_ADDRESSES);
//            fw.write(addrs);
//            fw.close();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        // setup server and client for communication with other nodes
        if(mServer != null){
            mServer.release();
        }
        mServer = new CommunicationServer();
        mServer.setup();

        if(mClient != null) {
            mClient.release();
        }
        mClient = new CommunicationClient();
        mClient.setup();

        // wait for setting up of all nodes
        try {
            sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ConnectTasks ct = new ConnectTasks();
        ct.execute(assignment, mClient);

        while(allConnected==0){     // wait until all workers are connected
            try {
                sleep(5);
                logger.info("Waiting workers to connect with each other ... ");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(allConnected==-1){
            logger.error("Some connections between workers failed ... ");
            Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, "Workers cannot establish connections, please turn off and try again ...").sendToTarget();
            return START_NOT_STICKY;
        } else{
            logger.info("All connections among workers succeed ... ");
        }

        // execute tasks assigned to this node
        HashMap<String, String> component2serInstance = topology.getSerInstances();
        String fileName = assignment.getApk();
        File dexOutputDir = this.getApplicationContext().getFilesDir();
        DexClassLoader dcLoader = new DexClassLoader(MStorm.apkFileDirectory + fileName, dexOutputDir.getAbsolutePath(), null, this.getClassLoader());
        if (localTasks!=null) {
            HashMap<Integer, String> task2Component = assignment.getTask2Component();
            for (int i = 0; i < localTasks.size(); i++) {
                int taskID = localTasks.get(i);
                String component = task2Component.get(taskID);
                String sourceAddr  = assignment.getSourceAddr();  // GUID addr
                String sourceIP  = GNSServiceHelper.getIPInUseByGUID(sourceAddr);
                String instance = component2serInstance.get(component);
                try {
                    Class<?> mClass = dcLoader.loadClass(component);
                    BTask mTask = (BTask) Serialization.Deserialize(instance, mClass);
                    mTask.setTaskID(taskID);
                    mTask.setComponent(component);
                    mTask.setSourceIP(sourceIP);
                    System.out.println("###############" + component + "###############" + sourceIP + "\n");
                    Executor taskExecutor = new Executor(mTask);
                    mExecutorManager.submitTask(taskID,taskExecutor);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }


        // wait for all tasks running up
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        dispatcher = new Dispatcher();
        Thread dispatchThread = new Thread(dispatcher);
        dispatchThread.setPriority(Thread.MAX_PRIORITY);
        dispatchThread.start();

        reporterThread = new Thread(statusReporter);
        reporterThread.setPriority(Thread.MAX_PRIORITY);
        reporterThread.start();

        // Start this service as foreground service
        Notification.Builder builder = new Notification.Builder (this.getApplicationContext());
        Intent nfIntent = new Intent(this, MStorm.class);
        builder.setContentIntent(PendingIntent.getActivity(this, 0, nfIntent, 0))
                .setLargeIcon(BitmapFactory.decodeResource(this.getResources(), R.mipmap.ic_large))
                .setContentTitle("ComputingNode Is Running")
                .setSmallIcon(R.mipmap.ic_launcher)
                .setContentText("ComputingNode Is Running")
                .setWhen(System.currentTimeMillis());
        Notification notification = builder.build();
        notification.defaults = Notification.DEFAULT_SOUND;
        startForeground(101, notification);

        return START_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return mBinder;
    }

    @Override
    public void onDestroy() {
        Toast.makeText(this, "Computing Node Shut down", Toast.LENGTH_SHORT).show();

        // stop client and server for communication
        if (mClient != null) {
            mClient.release();
            mClient = null;
        }
        if (mServer != null) {
            mServer.release();
            mServer = null;
        }

        // stop the executors
        List<Runnable> threadsInExecutor = mExecutorManager.shutdownNow();
        for(Runnable thread: threadsInExecutor){
            if(thread instanceof Executor)
                ((Executor) thread).stop();
        }

        try {
            System.out.println("Wait all threads in executor pool to stop ... ");
            Thread.sleep(100);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        // release sampling resource
        if(StatusReporter.getInstance()!=null)
            StatusReporter.getInstance().stopSampling();
        // stop packet dispatcher
        dispatcher.stop();
        // stop status reporter
        reporterThread.interrupt();
        // clear computation status
        MessageQueues.removeTaskQueues();
        // clear channel status
        ChannelManager.releaseChannelsToRemote();

        stopForeground(true);
        super.onDestroy();
    }

    private class ConnectTasks extends AsyncTask<Object, Void, Void> {
//        @Override
//        protected Void doInBackground(Object... objects) {
//            Assignment assignment = (Assignment) objects[0];
//            CommunicationClient mClient = (CommunicationClient) objects[1];
//            ArrayList<Integer> localTasks = assignment.getNode2Tasks().get(MStorm.GUID);
//            HashMap<Integer, MyPair<Integer, Integer>> port2TaskPair = assignment.getPort2TaskPair();
//            HashMap<Integer, String> task2Node = assignment.getTask2Node();
//
//            if (localTasks != null) {
//                for (int i = 0; i < localTasks.size(); i++) {
//                    int taskId = localTasks.get(i);
//                    Iterator<Map.Entry<Integer, MyPair<Integer, Integer>>> it = port2TaskPair.entrySet().iterator();
//                    while (it.hasNext()) {
//                        Map.Entry<Integer, MyPair<Integer, Integer>> entry = it.next();
//                        if (entry.getValue().getL().equals(taskId)) {
//                            int localPort = entry.getKey();
//                            int remoteTaskId = entry.getValue().getR();
//                            String remoteAddress = task2Node.get(remoteTaskId);  // address in GUID
//                            mClient.addlocalPort2RemoteGUID(localPort,remoteAddress);
//                            ChannelFuture cf = mClient.connect(localPort);
//                            cf.awaitUninterruptibly();
//                            Channel currentChannel = cf.getChannel();
//                            if(cf.isSuccess() && currentChannel!=null && currentChannel.isConnected()){
//                                String msg = "A connection from " + currentChannel.getLocalAddress().toString() + " to " + currentChannel.getRemoteAddress().toString() + " succeeds ... ";
//                                logger.debug(msg);
//                            } else{
//                                if(currentChannel!=null){
//                                    currentChannel.close();
//                                }
//                                allConnected = -1;
//                            }
//                        }
//                    }
//                }
//            }
//            allConnected = 1;
//            return null;
//        }

        @Override
        protected Void doInBackground(Object... objects) {
            Assignment assignment = (Assignment) objects[0];
            CommunicationClient mClient = (CommunicationClient) objects[1];
            List<String> assignNodes = assignment.getAssginedNodes();
            int numOfNodes = assignNodes.size();
            String localAddr = MStorm.GUID;
            int indexOfLocalNode;
            if ((indexOfLocalNode = assignNodes.indexOf(localAddr)) != -1){
                int[][] node2NodeConnection = assignment.getNode2NodeConnection();
                for (int i = 0; i < numOfNodes; i++) {
                    if (node2NodeConnection[indexOfLocalNode][i] == 1) {
                        String remoteGUID = assignNodes.get(i);
                        ChannelFuture cf = mClient.connectByGUID(remoteGUID);
                        cf.awaitUninterruptibly();
                        Channel currentChannel = cf.getChannel();
                        if (cf.isSuccess() && currentChannel != null && currentChannel.isConnected()) {
                            String msg = "A connection from " + currentChannel.getLocalAddress().toString() + " to " + currentChannel.getRemoteAddress().toString() + " succeeds ... ";
                            logger.debug(msg);
                        } else {
                            if (currentChannel != null) {
                                currentChannel.close();
                            }
                            allConnected = -1;
                        }
                    }
                }
            }
            allConnected = 1;
            return null;
        }
    }

    // get processID of computing node
    public static int getProcessID(){
        return processID;
    }

    public static int getPauseOrContinue(){return pauseOrContinue;}

    public static void setPauseOrContinue(int p){pauseOrContinue = p;}

    // get assignment
    public static Assignment getAssignment() {
        return assignment;
    }

    public static Topology getTopology(){
        return topology;
    }
}
