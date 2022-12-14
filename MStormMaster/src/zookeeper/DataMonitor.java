package zookeeper;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.util.internal.CaseIgnoringComparator;

import cluster.Cluster;

import com.google.gson.Gson;

public class DataMonitor implements Watcher, StatCallback ,ChildrenCallback, StringCallback, DataCallback{

    Logger logger = Logger.getLogger("DataMonitor");
    
    private static final String CLUSTER_ZNODE="/clusters";
    private static final String ASSIGN_ZNODE="/assignments";
    private static final String NODES_ZNODE="/nodes";
    private static final String NODE_CHANGE_PARTERN = CLUSTER_ZNODE+"/\\d+"+NODES_ZNODE;

    private ZooKeeper zk;
    private Watcher chainedWatcher;
    boolean dead=false;
    boolean initiated=false;
    DataMonitorListener listener;

    public DataMonitor(ZooKeeper zk,Watcher chainedWatcher, DataMonitorListener listener) {
        this.zk = zk;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        
        //zk.exists(tasks_znode, false, this, null);
        zk.exists(CLUSTER_ZNODE, false, this, null);
        //zk.getData(nodes_znode, true, this, null);

    }


    public void addNewAssignment(Assignment ass, int id){
    	 String serAssgnm= new Gson().toJson(ass);
    	 byte[] b = serAssgnm.getBytes(Charset.forName("UTF-8"));
    	 zk.create(getAssgDir(id)+"/"+ass.getAssignId(),b, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, this, null);
    	 
    }
    
    public void updateAssignment(Assignment ass, int id){
    	 String serAssgnm= new Gson().toJson(ass);
    	 byte[] b = serAssgnm.getBytes(Charset.forName("UTF-8"));
    	 zk.setData(getAssgDir(id)+"/"+ass.getAssignId(),b, -1, this, null);
    }
    
    public void deleteAssignment(int assId, int clusterId ) {
    	 try {
			zk.delete(getAssgDir(clusterId)+"/"+assId, -1);
		} catch (InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface DataMonitorListener {
        /**
         * The existence status of the node has changed.
         */
        void exists(byte data[]);

        /**
         * The ZooKeeper session is no longer valid.
         *
         * @param rc
         *                the ZooKeeper reason code
         */
        void closing(int rc);
    }

    /**event callback for Zookeeper Client*/
    public void process(WatchedEvent event) {
    	logger.debug("Event state ============================" + event.getState()); 	
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            	case SyncConnected:
            		logger.info("Connectted to the Zookeeper server!\n");
            		break;
            	case Expired:
            		// It's all over
            		ZookeeperClient zkClient = ((ZookeeperClient) listener);
                    zkClient.stopZookeeperClient();
                    logger.info("Zookeeper session expired, trying reconnecting ... ");
                    zkClient.connect();
                    while(!zkClient.isConnected()){
                        try {
                            Thread.sleep(500L);
                            logger.info("Waiting for zkClient reconnecting ... ");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    logger.info("Successfully reconnect to Zookeeper... ");
					try {
						zkClient.createCluster(Cluster.getCluster());
						logger.info("Successfully watch Zookeeper after reconnecting ... ");
					} catch (KeeperException | InterruptedException e) {
						logger.debug("Cannot watch Zookeeper after reconnecting ... ");
						e.printStackTrace();
					}
            		break;
            	default:
            		break;
            }
        } else {
            if (path != null && path.matches(NODE_CHANGE_PARTERN)) {
            	logger.info("processevent,getChildren ************************" + path);
             	zk.getChildren(path, true, this, null);
            }
        }
        
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }
    
    //Exist call back
    @SuppressWarnings("deprecation")
	@Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;
        switch (rc) {        
        case Code.Ok:
            exists = true;
            break;
        case Code.NoNode:
            exists = false;
            break;
        case Code.SessionExpired:
        case Code.NoAuth:
            dead = true;
            listener.closing(rc);
            return;
        default:
            // Retry errors
            zk.exists(path, true, this, null);
            return;
        }
        if(!exists)
        {
        	zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, this, null);
        }
    }
    
       
    //zookeeper.getChildren() Callback  
    @SuppressWarnings("deprecation")
	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) {
        boolean exists;
        switch (rc) {  
        	case Code.Ok:
        		exists = true;
        		break;
        	case Code.NoNode:
        		exists = false;
        		break;
        	case Code.SessionExpired:
        	case Code.NoAuth:
        		dead = true;
        		listener.closing(rc);
        		return;
        	default:
        		// Retry errors
        		zk.getChildren(path, true, this, null);
        		return;
        	}
       
        	if(path.matches(NODE_CHANGE_PARTERN)) {
        		logger.info("Cluster changed! Current Cluster size:"+children.size());       
        		Cluster.getClusterById(getClusterIdFromPath(path)).updateComputingNodes(children);	
        	}

		// TODO Auto-generated method stub	
        
	}
    

	//zookeeper.create() call back
	@Override
	public void processResult(int rc, String path, Object ctx, String name) {
		// TODO Auto-generated method stub
		boolean exists=false;
        switch (rc) {  
    	case Code.Ok:
    		break;
    	case Code.NoNode:
    		exists = false;
    		break;
    	case Code.SessionExpired:
    	case Code.NoAuth:
    		dead = true;
    		listener.closing(rc);
    		return;
    	default:
    		// Retry errors
    		logger.info("Create to directory "+path+"Error");
    		zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, this, null);
    		return; 	
        }
	}
	
	//zookeeper.getdata() call back
	@Override
	public void processResult(int arg0, String arg1, Object arg2, byte[] arg3, Stat arg4) {
		// TODO Auto-generated method stub
		
	}
	
	private int getClusterIdFromPath(String path){
		int index1=path.indexOf('/',1);
		int index2=path.indexOf('/',index1+1);
		
		return Integer.parseInt(path.substring(index1+1,index2));

	}
	
	private ArrayList<String> getDiff(List<String> moreNodes, List<String> lessNodes)
	{
		HashSet<String> temp=new HashSet<String>();
		for(int i=0;i<lessNodes.size();i++)
		{
			temp.add(lessNodes.get(i));
		}
		ArrayList<String> result=new  ArrayList<String>();
		for(int i=0;i<moreNodes.size();i++)
		{
			if(!temp.contains(moreNodes.get(i)))
			{
				result.add(moreNodes.get(i));
			}
		}
		return result;
	}
	
	private String getClusterDir(int id){
		return CLUSTER_ZNODE + "/"+ id;
	}
	
	private String getNodesDir(int id){
		return getClusterDir(id) +NODES_ZNODE;
	}
	
	private String getAssgDir(int id){
		return  getClusterDir(id) +ASSIGN_ZNODE;
	}
	
	public void createCluster(Cluster cluster) throws KeeperException, InterruptedException {
		int clusterId=cluster.getClusterId();
		
		if(zk.exists(getClusterDir(clusterId), false)==null)
			zk.create(getClusterDir(clusterId), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,  CreateMode.PERSISTENT);
		
		if(zk.exists(getAssgDir(clusterId), false)==null)
			zk.create(getAssgDir(clusterId), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		if(zk.exists(getNodesDir(clusterId), false)==null)
			zk.create(getNodesDir(clusterId), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		zk.getChildren(getNodesDir(clusterId), true, this, null);
	}
	
	public void deleteCluster(Cluster cluster) throws KeeperException, InterruptedException {
		int clusterId=cluster.getClusterId();
		zk.delete(getNodesDir(clusterId), -1);
		zk.delete(getAssgDir(clusterId), -1);
		zk.delete(getClusterDir(clusterId), -1);
	}
}
