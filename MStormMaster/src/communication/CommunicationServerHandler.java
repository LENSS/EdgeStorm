package communication;

import masternode.MasterNode;
import nimbusscheduler.NimbusScheduler;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.xbill.DNS.Master;

import com.google.gson.Gson;

import status.ReportToNimbus;
import utils.Serialization;
import zookeeper.Assignment;
import cluster.Cluster;
import cluster.Node;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class CommunicationServerHandler extends SimpleChannelHandler {

	static Semaphore semaphore = new Semaphore(1);

	static Logger logger = Logger.getLogger(CommunicationServerHandler.class);

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss");

	/** Session is connected! */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		ChannelManager.addRec(ctx.getChannel());
		// Logging
		String loggerChannelConnectionMsg = ctx.getChannel().getRemoteAddress().toString() + " is connected!" + "\n\n";
		logger.info(loggerChannelConnectionMsg);
	}

	/** Some message was delivered */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		/*
		 * String strReq = (String) e.getMessage(); Request recReq=
		 * Serialization.Deserialize(strReq, Request.class);
		 */
		Request recReq = (Request) e.getMessage();
		if (recReq == null)
			return;
		int type = recReq.getReqType();
		switch (type) {
		case Request.GETZOOKEEPER:
			String loggerGetZooKeeperAddr = "Request Zookeeper Address!" + "\n\n";
			logger.info(loggerGetZooKeeperAddr);		
			Reply reply_zookeeper = new Reply();
			reply_zookeeper.setType(Reply.ZOOKEEPERADDR);
			reply_zookeeper.setContent(MasterNode.zooKeeperConnectionString);
			ctx.getChannel().write(reply_zookeeper);
			logger.info("Zookeeper Address " + MasterNode.zooKeeperConnectionString + " is sent!" + "\n\n");
			break;
		case Request.JOIN:
			// Logging
			String loggerJoinMsg = "Request to join mStorm platform!" + "\n\n";
			logger.info(loggerJoinMsg);		
			Reply reply = new Reply();
			try {
				Cluster cluster = Cluster.getCluster();
				MasterNode.getInstance().mZkClient.createCluster(cluster);
				reply.setType(Reply.CLUSTER_ID);
				int clusterId = cluster.getClusterId();
				reply.setContent(Integer.toString(clusterId));
			} catch (KeeperException | InterruptedException e1) {
				reply.setType(Reply.FAILED);
				reply.setContent(e1.getMessage());
				e1.printStackTrace();
			}
			ctx.getChannel().write(reply);
			break;		
		case Request.TOPOLOGY:
			// Logging
			String loggerNewTopologyMsg = "New topology is received!" + "\n\n";
			logger.info(loggerNewTopologyMsg);

			// can be commented out for exercise by putting apk files in different phones
			// !!!!!!!!!!!!!!!!!!!!!!
			//			FileClient fileClient = new FileClient(System.getProperty("user.home") + "/apkFiles");
			//			String fileName = recReq.getFileName();
			//			fileClient.requestFile(fileName, recReq.getIP()); // get the apk file from User
			//			new Thread(fileClient).start();
			//
			//			while (!FileClientHandler.FileOnServer) {
			//				try {
			//					System.out.println("Client is uploading apk file ...");
			//					Thread.sleep(1000);
			//				} catch (InterruptedException e1) {
			//					// TODO Auto-generated catch block
			//					e1.printStackTrace();
			//				}
			//			}
			//			System.out.println("Apk file is in server now!");
			// can be commented out for exercise by putting apk files in different phones
			// !!!!!!!!!!!!!!!!!!!!!!!

			Cluster curCluster = Cluster.getClusterByNodeAddress(recReq.getGUID());
			// Start to Schedule
			NimbusScheduler sch = MasterNode.getInstance().mNimbusScheduler;
			Assignment assign = sch.fstSchedule(recReq);
			int topologyId = assign.getAssignId();
			curCluster.setTopologyBeingScheduled(topologyId, false);
			Reply reply2 = new Reply();
			reply2.setType(Reply.TOPOLOGY_ID);
			reply2.setContent(Integer.toString(topologyId));
			ctx.getChannel().write(reply2);
			break;
		case Request.CANCEL:
			// Logging
			String loggerCancelTopologyMsg = "Cancel Topology" + "\n\n"; 
			logger.info(loggerCancelTopologyMsg);
			Cluster curClusterCancel =  Cluster.getClusterByNodeAddress(recReq.getGUID());
			String topologyIdCancel = recReq.getContent();
			if(curClusterCancel!=null)
				curClusterCancel.deleteAssignment(Integer.valueOf(topologyIdCancel));
			break;
		case Request.GETAPKFILE:
			// Logging
			String loggerNewAPKFileMsg = "New Request Recived! Request Type is Request for APK file!" + "\n\n"; 
			logger.info(loggerNewAPKFileMsg);

			synchronized (FileServer.ServerExist) {
				if (!FileServer.ServerExist) {
					FileServer fileServer = new FileServer(System.getProperty("user.home") + "/apkFiles");
					fileServer.setup();
					new Thread(fileServer).start();
					FileServer.ServerExist = true;
				}
			}
			//// For windows system
			// InetSocketAddress fileSocketAddress = (InetSocketAddress)
			//// ctx.getChannel().getLocalAddress();
			// String fileAddress = fileSocketAddress.getAddress().toString().substring(1);

			//// For linux System
			String fileAddress = ((InetSocketAddress) ctx.getChannel().getLocalAddress()).getHostName();
			Reply reply3 = new Reply();
			reply3.setType(Reply.GETAPK);
			reply3.setAddress(fileAddress);
			reply3.setContent(recReq.getFileName());
			ctx.getChannel().write(reply3);
			break;
		case Request.PHONESTATUS:
			String statusReportStr = recReq.getContent();
			String phoneAddress = recReq.getGUID();

			// Logging
			String loggerNewStatusReportMsg = "A Status Report from GUID: " + phoneAddress + "\n" + "The Report is: " + statusReportStr + "\n\n"; 
			logger.info(loggerNewStatusReportMsg);

			// temporary comment out for March exercise, need more debugging later
//			int cluster_id = Integer.parseInt(recReq.getClusterID());
//			Cluster cluster = Cluster.getClusterById(cluster_id);
//			if(cluster != null) {
//				// Update report at server  
//				ReportToNimbus statusReport = (ReportToNimbus) Serialization.Deserialize(statusReportStr, ReportToNimbus.class);			
//				int nodeId = cluster.getNodeIDByAddr(phoneAddress);
//				Node node = cluster.getNodeByNodeId(nodeId);
//				try {
//					semaphore.acquire();
//					node.updateStatus(statusReport);
//				} catch (InterruptedException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				} finally {
//					semaphore.release();
//				}
//
//				// Check if reschedule is needed
//				int topoId = cluster.getTopologyIdByNodeId(nodeId);
//				if (topoId != -1) {
//					if (cluster.meetConditionForReScheduling(topoId)) {
//						cluster.setTopologyBeingScheduled(topoId, true);
//						NimbusScheduler scheduler = MasterNode.getInstance().mNimbusScheduler;
//						scheduler.reSchedule(topoId, cluster);
//						cluster.setTopologyBeingScheduled(topoId, false);
//					}
//				}
//			}
//			// temporary comment out for March exercise, need more debugging later
			break;
		default:
			return;
		}
	}

	/** An exception is occurred! */
	/** Here we need to consider reconnect to the server */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {

		e.getCause().printStackTrace();

		if (ctx.getChannel() != null) {
			ctx.getChannel().close();
		}
	}

	/** The channel is going to closed. */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		// LogByCodeLab.w(String.format("%s.channelClosed()",
		// NetworkEventHandler.class.getSimpleName()));
		super.channelClosed(ctx, e);

		logger.info(ctx.getChannel().getRemoteAddress().toString() + " connection closed!");
	}
}
