package com.lenss.mstorm.communication.masternode;

import com.lenss.mstorm.core.MStormWorker;
import com.lenss.mstorm.core.Supervisor;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;


public class MasterNodeClientHandler extends SimpleChannelHandler {
	private final String TAG="MasterNodeClientHandler";
	Logger logger = Logger.getLogger(TAG);
//	private FileClient fileClient;
	MasterNodeClient masterNodeClient;
	
	public MasterNodeClientHandler(MasterNodeClient masterNodeClient){
		this.masterNodeClient = masterNodeClient;
	}

	/** Session is connected! */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		logger.info("Connect to " + ctx.getChannel().getRemoteAddress().toString() + " successfully!");
		masterNodeClient.setChannel(ctx.getChannel());
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		Reply reply=(Reply)e.getMessage();
		int type=reply.getType();
		switch(type) {
			case Reply.CLUSTER_ID:
				Supervisor.mHandler.handleMessage(Supervisor.CLUSTER_ID, reply.getContent());
				break;
			case Reply.FAILED:	
				logger.info("Topology cannot be scheduled!");
				break;
			case Reply.TOPOLOGY_ID:
				logger.info("Topology has been scheduled!");
				masterNodeClient.setReply(reply);
				break;
			/// Can be commented out for real exercise !!!!!!!!!!!!!!!
//			case Reply.GETAPK: // Reply to Mobile Client in User's app only
//				logger.info("Request apk file from master!");
//				fileClient = new FileClient(GReporter.MStormDir+"APK");
//				fileClient.requestFileSetup(reply.getContent(), reply.getAddress());
//				new Thread(fileClient).start();
//				break;
			/// Can be commented out for real exercise !!!!!!!!!!!!!!!
			default:
				return;
		}
	}
	
	/** An exception is occurred!
	 *  Here we need to consider reconnect to the server*/
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		super.exceptionCaught(ctx, e);
		Channel ch = ctx.getChannel();

		ch.close();

		String exceptionMSG ="Try reconnecting to MStorm master ... ";
		if(Supervisor.mHandler!=null)
			Supervisor.mHandler.handleMessage(Supervisor.Message_LOG,exceptionMSG);
		logger.info(exceptionMSG);
		masterNodeClient.connect();
	}

	/** The channel is going to closed. */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
//		if(fileClient!=null) {
//			fileClient.release();   // release the resource of fileClient\
//		}
		super.channelClosed(ctx, e);
		logger.info(ctx.getChannel().getRemoteAddress().toString() + " connection closed!");
		masterNodeClient.setChannel(null);
	}
}
