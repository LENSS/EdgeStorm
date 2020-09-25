package com.lenss.mstorm.core;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.processing.SupportedSourceVersion;
import javax.imageio.spi.RegisterableService;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.junit.validator.PublicClassValidator;

import com.google.gson.Gson;
import com.lenss.mstorm.communication.masternode.MasterNodeClient;
import com.lenss.mstorm.communication.masternode.Request;
import com.lenss.mstorm.zookeeper.Assignment;
import com.lenss.mstorm.zookeeper.ZookeeperClient;

import sun.java2d.pipe.SpanClipRenderer;

public class Supervisor{
	Logger logger = Logger.getLogger("Supervisor");
	
	public static final int Message_LOG = 0;
    public static final int Message_GOP_RECVD = 1;
    public static final int Message_GOP_SEND = 2;
    public static final int Message_PERIOD_REPORT = 3;
    public static final int CLUSTER_ID = 4;
	
    
    private ZookeeperClient mZKClient = null;
	private boolean isRuning =false;
	public static MasterNodeClient masterNodeClient;
	public static String cluster_id;
	public static Assignment newAssignment;
	public ComputingNode computingNode;
	
	public static Handler mHandler;
	
	public interface Handler{ public void handleMessage(int msg_t, String msg_c);}
	
	public void onStartCommand() {
		mHandler = new Handler() {
			@Override
			public void handleMessage(int msg_t, String msg_c ) {
				switch(msg_t) {
					case Message_LOG:
						logger.info(msg_c);
						break;
					case CLUSTER_ID:
						register(msg_c);
						logger.info("Have registered to MStormMaster");
						break;
					case Message_GOP_RECVD:
						logger.info("Received "+ msg_c + "bytes!");
						break;
					case Message_GOP_SEND:
						logger.info("Sent "+ msg_c + "bytes!");
						break;		
				}
			}
		};
		
		// Start master node client
		masterNodeClient = new MasterNodeClient(MStormWorker.MASTER_NODE_GUID);
		masterNodeClient.connect();
		// waiting for connection
        while(!masterNodeClient.isConnected()){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("=================MStorm Master is connected=========================");
        
        // get zookeeper address
        requestZooKeeperAddrFromMaster();
        
        // wait for getting Zookeeper address
        while(MStormWorker.ZK_ADDRESS_IP==null){
        	logger.info("=================Wait for getting Zookeeper address from MStorm master=========================");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("=================Get Zookeeper address from MStorm master already=========================");
     
		// Start zookeeper client
		try {
			mZKClient = new ZookeeperClient(this, MStormWorker.ZK_ADDRESS_IP);
			new Thread(mZKClient).start();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
        // wait for zookeeper connection
        while(!mZKClient.isConnected()){
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("=================Zookeeper is connected=========================");
		
        // join cluster
		joinCluster();
	}
	
	public void onDestroy() {
		if(mZKClient!=null)
        {
            if(cluster_id!=null){
                unregister(cluster_id);
            }
            mZKClient.stopZookeeperClient();
            mZKClient=null;
            if(Supervisor.mHandler!=null) {
                Supervisor.mHandler.handleMessage(Supervisor.Message_LOG, "Disconnected to Zookeeper finally!");
            }
        }
	}
	
	public void joinCluster() {
        Request req=new Request();
        req.setReqType(Request.JOIN);
        req.setIP(MStormWorker.localAddress);
        masterNodeClient.sendRequest(req);
    }
	
    public void requestZooKeeperAddrFromMaster(){
        Request req=new Request();
        req.setReqType(Request.GETZOOKEEPER);
        req.setIP(MStormWorker.localAddress);
        masterNodeClient.sendRequest(req);
    }
    
    public void register(String cluster_id){
        this.cluster_id=cluster_id;
        mZKClient.register(cluster_id);
    }
	    
    public void unregister(String cluster_id){
        mZKClient.unregister(cluster_id);
    }
    
	public void startComputing(String assignment) {
		newAssignment=new Gson().fromJson(assignment, Assignment.class);
		
		if(!isRuning) { // the computing service is not running, start it
            if (newAssignment.getAssginedNodes().contains(MStormWorker.GUID)) {
                mHandler.handleMessage(Message_LOG, "New Assignment, start computing!");
                computingNode = new ComputingNode(newAssignment);
                new Thread(computingNode).start();
                isRuning = true;
            }
        } else {
        	// TO DO
        }
	}
	
	public void stopComputing() {
		if(isRuning) {
            Supervisor.mHandler.handleMessage(Supervisor.Message_LOG,"Stop computing!");
            isRuning=false;
            computingNode.stop();
        }
	}
	
	public void stopComputing(String assignment) {
		Assignment cancelAssign=new Gson().fromJson(assignment, Assignment.class);
        if(cancelAssign.getAssginedNodes().contains(MStormWorker.GUID)){
            stopComputing();
        }
	}	
}

