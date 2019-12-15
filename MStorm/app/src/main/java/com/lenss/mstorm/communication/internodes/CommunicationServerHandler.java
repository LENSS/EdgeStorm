package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.status.StatusOfDownStreamTasks;
import com.lenss.mstorm.utils.MyPair;
import com.lenss.mstorm.zookeeper.Assignment;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import java.net.InetSocketAddress;
import java.util.HashMap;


public class CommunicationServerHandler extends SimpleChannelHandler {
	private final String TAG="CommunicationServerHandler";
	Logger logger = Logger.getLogger(TAG);

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		Channel ch = ctx.getChannel();
		String channelConnectedMSG = "P-server " + ((InetSocketAddress) ch.getLocalAddress()).getAddress().getHostAddress()
					+ " connects to P-client " + ((InetSocketAddress) ch.getRemoteAddress()).getAddress().getHostAddress();
		Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, channelConnectedMSG).sendToTarget();
		logger.info(channelConnectedMSG);

		// Send the first packet to tell the client about the server's GUID
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
				logger.debug("Init pkt received from:" + ctx.getChannel().getRemoteAddress());
				ChannelManager.addChannelToRemote(ctx.getChannel(), pkt.simpleContent.get("GUID"));
			} else if(pkt.type == InternodePacket.TYPE_DATA){
				logger.debug("Data pkt received from:" + ctx.getChannel().getRemoteAddress());
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
		super.exceptionCaught(ctx, e);
		Channel ch = ctx.getChannel();

		// a connected channel gets disconnected
		if(ch!=null && ch.getRemoteAddress()!=null) {
			String channelClosedMSG = "P-server " + ((InetSocketAddress)ch.getLocalAddress()).getAddress().getHostAddress()
					+ " disconnects to P-client " + ((InetSocketAddress) ch.getRemoteAddress()).getAddress().getHostAddress();
			Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,channelClosedMSG).sendToTarget();
			logger.info(channelClosedMSG);
		}

		// remove the record of the channel
		if(ChannelManager.channel2RemoteGUID.containsKey(ch.getId()))
			ChannelManager.removeChannelToRemoteGUID(ch);

		// close the channel
		ch.close();
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelClosed(ctx, e);
	}
}
