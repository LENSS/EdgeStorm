package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.status.StatusOfDownStreamTasks;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;


public class CommunicationClientHandler extends SimpleChannelHandler {
	private final String TAG="CommunicationClientHandler";
	Logger logger = Logger.getLogger(TAG);
	CommunicationClient communicationClient;

	public CommunicationClientHandler(CommunicationClient communicationClient) {
        this.communicationClient = communicationClient;
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		// Try connecting with maximum times
		communicationClient.reconnectedTimes = 0;

		super.channelConnected(ctx, e);
		Channel ch = ctx.getChannel();
		String channelConnectedMSG = "P-client " + ((InetSocketAddress)ch.getLocalAddress()).getAddress().getHostAddress()
								   + " connects to P-server " + ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
		Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,channelConnectedMSG).sendToTarget();
		logger.info(channelConnectedMSG);

		// Send the first packet to tell the server about the client's GUID
		InternodePacket pkt = new InternodePacket();
		pkt.type = InternodePacket.TYPE_INIT;
		pkt.simpleContent.put("GUID", MStorm.GUID);
		ch.write(pkt);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		super.messageReceived(ctx, e);
		InternodePacket pkt=(InternodePacket) e.getMessage();
		if(pkt!=null) {
			if(pkt.type == InternodePacket.TYPE_INIT){
				logger.debug("Init pkt received from:"+ctx.getChannel().getRemoteAddress());
				ChannelManager.addChannelToRemote(ctx.getChannel(), pkt.simpleContent.get("GUID"));
			} else if(pkt.type == InternodePacket.TYPE_DATA){
				logger.debug("Data pkt received from:"+ctx.getChannel().getRemoteAddress());
				int taskID = pkt.toTask;
				if(StreamSelector.select(taskID)==StreamSelector.KEEP) {
					logger.debug("Data pkt kept!");
					MessageQueues.collect(taskID, pkt);
				}
			} else if (pkt.type == InternodePacket.TYPE_REPORT) {
				int taskID = pkt.fromTask;
				StatusOfDownStreamTasks.collectReport(taskID, pkt);
			} else if(pkt.type == InternodePacket.TYPE_ACK) {
				//Todo
			} else {
				logger.info("Incorrect packet type!");
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
//		// Try connecting with maximum times
		logger.debug("==== communicationClient reconnectedTimes ====" + communicationClient.reconnectedTimes);
		communicationClient.reconnectedTimes++;

		super.exceptionCaught(ctx, e);
		Channel ch = ctx.getChannel();

		// a connected channel gets disconnected
		if(ch!=null && ch.getRemoteAddress()!=null) {
			String channelDisconnectedMSG = "P-client " + ((InetSocketAddress)ch.getLocalAddress()).getAddress().getHostAddress()
					+ " disconnects to P-server " + ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
			Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,channelDisconnectedMSG).sendToTarget();
			logger.info(channelDisconnectedMSG);
		}

		// try reconnecting
		String reconnectingMSG = "Try reconnecting to P-server ... ";
		Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,reconnectingMSG).sendToTarget();
		logger.info(reconnectingMSG);

		if(e.getCause().getClass().getName().equals("java.io.IOException")) {  // Software caused connection abort (client side) or Connection reset by peer (server side)
			// Get current remoteIP
			String remoteIP = ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
			// Get remoteGUID and remove from channelManager
			String remoteGUID = ChannelManager.channel2RemoteGUID.get(ch.getId());
			// Remove the records from channelManager
			if(ChannelManager.channel2RemoteGUID.containsKey(ch.getId()))
				ChannelManager.removeChannelToRemote(ch);
			// Make sure remoteIP is the latest
			String newRemoteIP = GNSServiceHelper.getIPInUseByGUID(remoteGUID);
			if(newRemoteIP!=null && !newRemoteIP.equals(remoteIP)){
				remoteIP = newRemoteIP;
			}
			// add the new remoteIP--remoteGUID relation
			ChannelManager.tempIP2GUID.put(remoteIP, remoteGUID);

			// Try connecting with maximum times
			if(communicationClient.reconnectedTimes < communicationClient.MAX_RETRY_TIMES){
				communicationClient.connectByIP(remoteIP);
			} else {
				String stopRetryMSG = "Have tried reconnecting to P-server for " + communicationClient.MAX_RETRY_TIMES + " times, give up ... ";
				Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,stopRetryMSG).sendToTarget();
				logger.info(stopRetryMSG);
			}

//			// Try connecting
//			communicationClient.connectByIP(remoteIP);
		} else if(e.getCause().getClass().getName().equals("java.net.ConnectException") ||
				e.getCause().getClass().getName().equals("org.jboss.netty.channel.ConnectTimeoutException")){	// ConnectException: network is unreachable (client side is trying to reconnect) or connection timeout
			String errorMsg = e.getCause().getMessage();
			int startIndex = errorMsg.indexOf("/")+1;
			int endIndex = errorMsg.lastIndexOf(":");
			// Get current remoteIP
			String remoteIP = errorMsg.substring(startIndex, endIndex);
			// Get remoteGUID
			String remoteGUID = ChannelManager.tempIP2GUID.get(remoteIP);
			// Make sure remoteIP is the latest
			String newRemoteIP = GNSServiceHelper.getIPInUseByGUID(remoteGUID);
			if(newRemoteIP!=null && !newRemoteIP.equals(remoteIP)) {
				// remove the expired remoteIP--remoteGUID relation
				if(ChannelManager.tempIP2GUID.containsKey(remoteIP))
					ChannelManager.tempIP2GUID.remove(remoteIP);
				remoteIP = newRemoteIP;
				// add the new remoteIP--remoteGUID relation
				ChannelManager.tempIP2GUID.put(remoteIP, remoteGUID);
			}
			// Wait for next try
			Thread.sleep(communicationClient.TIMEOUT);

			// Try connecting
			if(communicationClient.reconnectedTimes < communicationClient.MAX_RETRY_TIMES){
				communicationClient.connectByIP(remoteIP);
			} else {
				String stopRetryMSG = "Have tried reconnecting to P-server for " + communicationClient.MAX_RETRY_TIMES + " times, give up ... ";
				Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,stopRetryMSG).sendToTarget();
				logger.info(stopRetryMSG);
			}

//			// Try connecting
//			communicationClient.connectByIP(remoteIP);
		}
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelClosed(ctx, e);
	}
}