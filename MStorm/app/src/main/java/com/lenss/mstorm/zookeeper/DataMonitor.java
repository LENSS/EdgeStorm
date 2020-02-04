package com.lenss.mstorm.zookeeper;

import android.provider.ContactsContract;
import android.util.Log;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.status.StatusReporter;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class DataMonitor implements Watcher,AsyncCallback.DataCallback, AsyncCallback.StatCallback,AsyncCallback.StringCallback,AsyncCallback.ChildrenCallback{

    public static final String CLUSTER_DIR = "/clusters";
    public static final String ASSIGN_DIR = "/assignments";
    public static final String NODES_DIR = "/nodes";
    private static final String ASSIGN_ADD_PATTERM = CLUSTER_DIR+"/\\d+"+ASSIGN_DIR;
    private static final String ASSIGN_CHANGE_PATTERM = CLUSTER_DIR+"/\\d+"+ASSIGN_DIR+"/\\d+";

    private final String TAG="DataMonitor";
    Logger logger = Logger.getLogger(TAG);

    private ZooKeeper zk;
    private Watcher chainedWatcher;
    boolean dead = false;
    private boolean initiated=false;
    private DataMonitorListener listener;

    private String assignPath = null;
    private String assignment = null;
    private AssignmentProcessor assignmentProcessor;


    public DataMonitor(ZooKeeper zk, AssignmentProcessor assignmentProcessor, Watcher chainedWatcher, DataMonitorListener listener) {
        this.assignmentProcessor=assignmentProcessor;
        this.zk = zk;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
    }

     // other classes use the DataMonitor by implementing this interface
    public interface DataMonitorListener {
        /**
         * The existence status of the node has changed.
         */
        void exists(byte data[]);

        /**
         * The ZooKeeper session is no longer valid.
         */
        void closing(int rc);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    logger.debug("Connected to Zookeeper!");
                    if(Supervisor.mHandler!=null)
                        Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"Connected to Zookeeper!").sendToTarget();
                    break;
                case Expired:
                    ZookeeperClient zkClient = ((ZookeeperClient) listener);
                    zkClient.stopZookeeperClient();
                    logger.debug("Zookeeper session expired, trying to reconnecting ... ");
                    if(Supervisor.mHandler!=null)
                        Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"Zookeeper session expired, trying to reconnecting ... ").sendToTarget();
                    zkClient.connect();
                    while(!zkClient.isConnected()){
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if(Supervisor.cluster_id!=null) {
                        zkClient.register(Supervisor.cluster_id);
                        zkClient.listenOnTaskAssignment(Supervisor.cluster_id);
                    }
                    break;
            }
        } else {
            if (path != null && path.matches(ASSIGN_ADD_PATTERM)) {
                logger.debug("processevent,getChildren ************************" + path);
                zk.getChildren(path, true, this, null);
            }

            if(path != null && path.matches(ASSIGN_CHANGE_PATTERM)){
                logger.debug("processevent,getData ************************" + path);
                zk.getData(path,true,this,null);
            }
        }

        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }

    // callback method for zookeeper.exist()
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;
        switch (rc) {
            case KeeperException.Code.Ok:
                exists = true;
                break;
            case KeeperException.Code.NoNode:
                exists = false;
                break;
            case KeeperException.Code.SessionExpired:
            case KeeperException.Code.NoAuth:
                dead = true;
                listener.closing(rc);
                return;
            default:
                // Retry errors
                zk.exists(path, true, this, null);
                return;
        }
    }

    // callback method for zookeeper.getChildren()
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        boolean success;
        switch (rc) {
            case KeeperException.Code.Ok:
                success = true;
                break;
            case KeeperException.Code.NoNode:
                success = false;
                break;
            case KeeperException.Code.SessionExpired:
            case KeeperException.Code.NoAuth:
                dead = true;
                listener.closing(rc);
                return;
            default:
                zk.getChildren(path, true, this, null);
                return;
        }
        if(path.equals(NODES_DIR)) {
            // TODO
        } else { // Assign Dir Changed
            if (!initiated) {
                initiated = true;
            } else {
                if(!children.isEmpty()) {
                    assignPath = children.get(children.size() - 1); // get the newest assignment
                    logger.debug("processResult, getChildren ************************" + path);
                    zk.getData(path + "/" + assignPath, true, this, null);
                } else {
                    assignmentProcessor.stopComputing();
                }
            }
        }
    }

    // callback method for zookeeper.create()
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {   // Create Directory call back
        switch (rc) {
            case KeeperException.Code.Ok:
                Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"Successfully join the cluster: " + path).sendToTarget();
                logger.info("Successfully join the cluster: " + path);
                break;
            case KeeperException.Code.NoNode:
                Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"No Node: "+path+" on Zookeeper");
                break;
            case KeeperException.Code.SessionExpired:
            case KeeperException.Code.NoAuth:
                dead = true;
                listener.closing(rc);
                return;
            default:
                // Retry errors
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, null);
                return;
        }
    }

    // callback method for zookeeper.getData()
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        boolean ok;
        switch (rc) {
            case KeeperException.Code.Ok:
                ok = true;
                break;
            case KeeperException.Code.NoNode:
                ok = false;
                break;
            case KeeperException.Code.SessionExpired:
            case KeeperException.Code.NoAuth:
                dead = true;
                listener.closing(rc);
                return;
            default:
                // Retry errors
                zk.getData(path, true, this, null);
                return;
        }

        if(ok) {
            String newAssignment = null;
            try {
                newAssignment = new String(data, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            if(newAssignment!=null) {
                //Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,"New assignment from"+path).sendToTarget();
                try {
                    OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new FileOutputStream(MStorm.ASSIGNMENT_URL, true));
                    outputStreamWriter.write(newAssignment);
                    outputStreamWriter.close();
                }
                catch (IOException e) {
                    logger.error("Fail to write assignment result to local file!");
                }
                assignmentProcessor.startComputing(newAssignment);
                assignment = newAssignment;
                logger.debug("processResult, getData ************************" + path);
            }
        } else {
            if(assignment!=null){
                assignmentProcessor.stopComputing(assignment);
                assignment = null;
            }
        }
    }

    public void listenOnTaskAssignment(String cluster_id){
        zk.getChildren(CLUSTER_DIR + "/" + cluster_id + ASSIGN_DIR, true, this, null);
    }

    public void register(String cluster_id) {
        zk.create(CLUSTER_DIR + "/" + cluster_id + NODES_DIR +"/" + MStorm.GUID + ":" + MStorm.isPublicOrPrivate + ":" + MStorm.availability,
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,this,null);
    }

    public void unregister(String cluster_id){
        try {
            zk.delete(CLUSTER_DIR + "/" + cluster_id + NODES_DIR +"/" + MStorm.GUID + ":" + MStorm.isPublicOrPrivate + ":" + MStorm.availability,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        // the last node delete all assignments
        try {
            List<String> remainingNodes = zk.getChildren(CLUSTER_DIR + "/" + cluster_id + NODES_DIR, false);
            if(remainingNodes.size() == 0){
                List<String> remainingAssignments = zk.getChildren(CLUSTER_DIR + "/" + cluster_id + ASSIGN_DIR, false);
                for(String assign:remainingAssignments)
                    zk.delete(CLUSTER_DIR + "/" + cluster_id + ASSIGN_DIR + "/" + assign,-1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

