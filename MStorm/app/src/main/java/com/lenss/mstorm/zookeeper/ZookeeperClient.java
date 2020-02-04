package com.lenss.mstorm.zookeeper;

import android.os.AsyncTask;

import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.status.StatusReporter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

public class ZookeeperClient implements Watcher, Runnable, DataMonitor.DataMonitorListener {
    private ZooKeeper zk;
    private DataMonitor dm;
    private String zookeeperAddr;
    private AssignmentProcessor assignmentProcessor;

    public ZookeeperClient(AssignmentProcessor ap, String zkAddr){
        zookeeperAddr = zkAddr;
        assignmentProcessor = ap;
    }

    public void listenOnTaskAssignment(String cluster_id){
        dm.listenOnTaskAssignment(cluster_id);
    }

    public void register(String cluster_id) {
        dm.register(cluster_id);
    }

    public void unregister(String cluster_id){
        dm.unregister(cluster_id);
    }

    public DataMonitor getDM(){
        return dm;
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void process(WatchedEvent event) {
        dm.process(event);
    }

    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    public void exists(byte[] data) {

    }

    public void connect(){
        new connect2Zookeeper().execute(assignmentProcessor, zookeeperAddr, MStorm.SESSION_TIMEOUT, this);
    }

    public void stopZookeeperClient(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        dm.dead = true;
        closing(KeeperException.Code.SessionExpired);
    }

    public boolean isConnected(){
        return zk!=null && zk.getState().isConnected();
    }

    private class connect2Zookeeper extends AsyncTask<Object, ZookeeperClient, ZookeeperClient> {
        protected ZookeeperClient doInBackground(Object... objects) {
            ZookeeperClient zkClient;
            try {
                AssignmentProcessor assignmentProcessor= (AssignmentProcessor) objects[0];
                String zkAddr = (String) objects[1];
                int sessionTimeOut = (int) objects[2];
                zkClient = (ZookeeperClient) objects[3];
                zk = new ZooKeeper(zkAddr, sessionTimeOut, zkClient);
                dm = new DataMonitor(zk, assignmentProcessor, null, zkClient);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
            return zkClient;
        }

        protected void onPostExecute(ZookeeperClient zkClient) {
            if (zkClient != null) {
                new Thread(zkClient).start();
            }
        }
    }
}
