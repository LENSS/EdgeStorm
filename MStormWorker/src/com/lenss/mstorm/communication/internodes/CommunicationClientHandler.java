package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.core.ComputingNode;
import com.lenss.mstorm.core.Supervisor;
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


public class CommunicationClientHandler extends SimpleChannelHandler {
	private final String TAG="CommunicationClientHandler";
	Logger logger = Logger.getLogger(TAG);


	/** Session is connected! */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		String msg = "Distribute Stream to "+ctx.getChannel().getRemoteAddress().toString();
		Supervisor.mHandler.handleMessage(Supervisor.Message_LOG, msg);
		logger.info( "Connect to " + ctx.getChannel().getRemoteAddress().toString() + " successfully!");

		// Add this connected channel to (Component, List<Channels>)
		Assignment assignment = ComputingNode.getAssignment();
		if (assignment != null) {
			int port = ((InetSocketAddress) ctx.getChannel().getLocalAddress()).getPort();
			HashMap<Integer, MyPair<Integer, Integer>> port2TaskPair = assignment.getPort2TaskPair();
			int remoteTaskID= port2TaskPair.get(port).right;
			String component = assignment.getTask2Component().get(remoteTaskID);
			synchronized (this) {
				ChannelManager.addComponent2SendChannels(component, ctx.getChannel());
			}
		}
	}

	/** Client receive reports from downstream tasks */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		super.messageReceived(ctx, e);
		byte[] reportData=(byte[]) e.getMessage();
		Channel ch = ctx.getChannel();

		// Adjust execution time at downstream servers according to report from downstream
		Assignment assignment = ComputingNode.getAssignment();
		if(assignment!=null){
			int port = ((InetSocketAddress) ch.getLocalAddress()).getPort();
			HashMap<Integer,MyPair<Integer,Integer>> port2TaskPair = assignment.getPort2TaskPair();
			int remoteTaskID = port2TaskPair.get(port).right;
			String component = assignment.getTask2Component().get(remoteTaskID);
			double[] data = Helper.byteArraytoDoubleArray(reportData);
			//System.out.println(data);
			double throughPut = data[0];
			double avgDelay = data[1];
			double waitingQueueLength = data[2];
			// using throughPut or avgDelay or others depends on how the feedback based stream grouping is implemented
			ChannelManager.updateComponentServerExeTime(component, ch, avgDelay);  //ms
			ChannelManager.updateComponentServerThroughput(component, ch, throughPut); //tuple/s
			ChannelManager.updateComponentServerWaitingQueue(component, ch, waitingQueueLength);
			ChannelManager.updateComponentServerLastReportUpdateTime(component, ch, System.nanoTime());
		}
	}
	
	/** An exception is occurred!
      * Here we need to consider reconnect to the server
	  */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		super.exceptionCaught(ctx, e);
		if(ctx.getChannel() != null && ctx.getChannel().isOpen()) {
			ctx.getChannel().close();
		} else {

		}
		//mComputingNode.reconnect(e.getChannel().getLocalAddress(),e.getChannel().getRemoteAddress());
	}

	/** The channel is going to closed. **/
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelClosed(ctx, e);
		if(ctx.getChannel().getRemoteAddress()!=null) {
			logger.info(ctx.getChannel().getRemoteAddress().toString() + " connection closed!");
			String msg = ctx.getChannel().getRemoteAddress().toString() + " connection closed!";
			Supervisor.mHandler.handleMessage(Supervisor.Message_LOG, msg);
		}
	}
}

