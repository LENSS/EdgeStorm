package com.lenss.mstorm.core;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.processing.SupportedSourceVersion;
import javax.imageio.spi.RegisterableService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import com.google.gson.Gson;
import com.lenss.mstorm.communication.masternode.MasterNodeClient;
import com.lenss.mstorm.communication.masternode.Request;
import com.lenss.mstorm.zookeeper.Assignment;
import com.lenss.mstorm.zookeeper.ZookeeperClient;

import sun.java2d.pipe.SpanClipRenderer;

public class Supervisor{
	public static final int Message_LOG = 0;
    public static final int Message_GOP_RECVD = 1;
    public static final int Message_GOP_SEND = 2;
    public static final int Message_PERIOD_REPORT = 3;
    public static final int CLUSTER_ID = 4;
	
    
    private ZookeeperClient mZKClient = null;
	private boolean isRuning =false;
	public static MasterNodeClient masterNodeClient;
	public static String cluster_id;
	public Assignment newAssignment;
	
	public static Handler mHandler;
	
	public interface Handler{ public void handleMessage(int msg_t, String msg_c);}
	
	public void onStartCommand() {
		mHandler = new Handler() {
			@Override
			public void handleMessage(int msg_t, String msg_c ) {
				switch(msg_t) {
					case Message_LOG:
						System.out.println(msg_c);
						break;
					case CLUSTER_ID:
						register(msg_c);
					    System.out.println("Have registered to MStormMaster");
						break;
					case Message_GOP_RECVD:
						System.out.println("Received "+ msg_c + "bytes!");
						break;
					case Message_GOP_SEND:
						System.out.println("Sent "+ msg_c + "bytes!");
						break;		
				}
			}
		};
		
		// Start zookeeper client
		try {
			mZKClient = new ZookeeperClient(this, MStormWorker.ZK_ADDRESS_IP);
			new Thread(mZKClient).start();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Start master node client
		masterNodeClient = new MasterNodeClient(MStormWorker.GUID);
		masterNodeClient.setup();
		masterNodeClient.connect();
		
        // join MStorm cluster
        while(!masterNodeClient.isConnected()){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
		
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
                new Thread(new ComputingNode(newAssignment)).start();
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
        }
	}
	
}

