package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.utils.MyPair;
import com.lenss.mstorm.zookeeper.Assignment;

import org.apache.log4j.Logger;
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

	/** Session is connected! */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		String msg = "Receive Stream from "+ctx.getChannel().getRemoteAddress().toString();
		Supervisor.mHandler.handleMessage(Supervisor.Message_LOG, msg);
		logger.info( ctx.getChannel().getRemoteAddress() + "is connected to server successfully!");

		// Add the channels taskID receives tuple from
		Assignment assignment = ComputingNode.getAssignment();
		if(assignment!=null){
			int port = ((InetSocketAddress)ctx.getChannel().getRemoteAddress()).getPort();
			HashMap<Integer,MyPair<Integer,Integer>> port2TaskPair = assignment.getPort2TaskPair();
			int taskID = port2TaskPair.get(port).right;
			ChannelManager.addTask2RecvChannels(taskID,ctx.getChannel());
		}
    }

	/** Some message was delivered */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
/*		PowerManager.WakeLock wakeLock = WakeLockWrapper.getWakeLockInstance(mComputingNode, CommunicationServerHandler.class.getSimpleName());
		wakeLock.acquire();*/
		try {
			super.messageReceived(ctx, e);
            byte[] gop=(byte[])e.getMessage();

			// Add received tuple to corresponding task incomingQueue
			Assignment assignment = ComputingNode.getAssignment();
			if(assignment!=null){
				int port = ((InetSocketAddress)ctx.getChannel().getRemoteAddress()).getPort();
				HashMap<Integer,MyPair<Integer,Integer>> port2TaskPair = assignment.getPort2TaskPair();
				int taskID = port2TaskPair.get(port).right;
				if(gop!=null) {
					ComputingNode.collect(taskID, gop);
				}
			}
		} finally {
			/*wakeLock.release();*/
		}
	}
	
	/** An exception is occurred!
	 *  Here we need to consider reconnect to the server*/
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		super.exceptionCaught(ctx, e);
		if(ctx.getChannel() != null && ctx.getChannel().isOpen()) {
			ctx.getChannel().close();
		} else {
           // TODO: Try to reconnect
           // mComputingNode.scheduleToReconnect();
		}
	}

	/** The channel is going to closed. */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelClosed(ctx, e);
		logger.info(ctx.getChannel().getRemoteAddress().toString() + " connection closed!");
		String msg= ctx.getChannel().getRemoteAddress().toString() + " connection closed!";
		Supervisor.mHandler.handleMessage(Supervisor.Message_LOG,msg);
	}
}
