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
		super.channelConnected(ctx, e);
		Channel ch = ctx.getChannel();
		ChannelManager.addChannelToRemote(ch);
		if(ChannelManager.channel2RemoteGUID.containsKey(ch.getId())){
			String channelConnectedMSG = "P-client " + ((InetSocketAddress)ch.getLocalAddress()).getAddress().getHostAddress()
									   + " connects to P-server " + ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
			Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,channelConnectedMSG).sendToTarget();
			logger.info(channelConnectedMSG);
		}
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		super.messageReceived(ctx, e);
		InternodePacket pkt=(InternodePacket) e.getMessage();
		if(pkt!=null) {
			if(pkt.type == InternodePacket.TYPE_DATA){
				int taskID = pkt.toTask;
				MessageQueues.collect(taskID, pkt);
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
		super.exceptionCaught(ctx, e);
		Channel ch = ctx.getChannel();

		// a connected channel gets disconnected
		if(ch!=null && ch.getRemoteAddress()!=null) {
			String channelDisconnectedMSG = "P-client " + ((InetSocketAddress)ch.getLocalAddress()).getAddress().getHostAddress()
					+ " disconnects to P-server " + ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
			Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,channelDisconnectedMSG).sendToTarget();
			logger.info(channelDisconnectedMSG);
		}

		logger.info("NEW EXCEPTION IN CLIENT HANDLER *****************" + e.getCause());

		// try reconnecting
		String reconnectingMSG = "Try reconnecting to P-server ... ";
		Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,reconnectingMSG).sendToTarget();
		logger.info(reconnectingMSG);

		String remoteIP;
		if(e.getCause().getClass().getName().equals("java.io.IOException")) {  // Software caused connection abort or Connection reset by peer
			remoteIP = ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
			String remoteGUID = ChannelManager.channel2RemoteGUID.get(ch.getId());
			String newRemoteIP = GNSServiceHelper.getIPInUseByGUID(remoteGUID);
			if(newRemoteIP!=null && !newRemoteIP.equals(remoteIP))
				remoteIP = newRemoteIP;
			if(ChannelManager.channel2RemoteGUID.containsKey(ch.getId())) // remove the record of the channel
				ChannelManager.removeChannelToRemote(ch);
			ch.close(); // close the channel
			communicationClient.connectByIP(remoteIP);
		} else if(e.getCause().getClass().getName().equals("java.net.ConnectException")){	// ConnectException: network is unreachable
			String errorMsg = e.getCause().getMessage();
			int startIndex = errorMsg.indexOf("/")+1;
			int endIndex = errorMsg.lastIndexOf(":");
			remoteIP = errorMsg.substring(startIndex, endIndex);
			Thread.sleep(communicationClient.TIMEOUT);
			if(!ch.isConnected()) {	// After TIMEOUT, ch is still not connected, close the channel and start a new one
				ch.close();
				communicationClient.connectByIP(remoteIP);
			}
		}
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelClosed(ctx, e);
	}
}