package com.lenss.mstorm.zookeeper;

import com.lenss.mstorm.core.MStormWorker;
import com.lenss.mstorm.core.Supervisor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

public class ZookeeperClient implements Watcher, Runnable, DataMonitor.DataMonitorListener {
    private ZooKeeper zk;
    private DataMonitor dm;

    public ZookeeperClient(Supervisor supervisor, String hostPort) throws KeeperException, IOException {
        zk = new ZooKeeper(hostPort, MStormWorker.SESSION_TIMEOUT, this);
        dm = new DataMonitor(supervisor, zk, null, this);
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
    
    public void stopZookeeperClient(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        dm.dead = true;
        closing(1);
    }

}
