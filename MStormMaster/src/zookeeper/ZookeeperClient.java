package zookeeper;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import cluster.Cluster;

public class ZookeeperClient  implements Watcher, Runnable, DataMonitor.DataMonitorListener {
	private DataMonitor dm;
	private ZooKeeper zk;

	public ZookeeperClient(String hostPort)  throws KeeperException, IOException {
		zk = new ZooKeeper(hostPort, 5000, this);
		dm = new DataMonitor(zk, null, this);
	}
    
    public DataMonitor getDM(){
    	return dm;
    }
    
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
    
    public void process(WatchedEvent event) {
        dm.process(event);
    }
    
    public void createCluster(Cluster cluster) throws KeeperException, InterruptedException{
    	dm.createCluster(cluster);
    }
    
    public void deleteCluster(Cluster cluster) throws KeeperException, InterruptedException{
    	dm.deleteCluster(cluster);
    }
        
    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }
    
    public void exists(byte[] data) {}
}
