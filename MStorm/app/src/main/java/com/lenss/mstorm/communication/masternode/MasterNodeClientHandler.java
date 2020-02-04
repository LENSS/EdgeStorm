package com.lenss.mstorm.communication.masternode;

import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import java.net.InetSocketAddress;

public class MasterNodeClientHandler extends SimpleChannelHandler {
	private final String TAG="MasterNodeClientHandler";
	Logger logger = Logger.getLogger(TAG);
	private FileClient fileClient;
	MasterNodeClient masterNodeClient;

	public MasterNodeClientHandler(MasterNodeClient masterNodeClient){
		this.masterNodeClient = masterNodeClient;
	}

	/** Session is connected! */
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		// Try connecting with maximum times
		masterNodeClient.reconnectedTimes = 0;

		super.channelConnected(ctx, e);
		if(Supervisor.mHandler!=null)	// For both Mobile Clients in User's app and mobile storm platform, so need check
	    	Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, "Connected to MStorm Master!").sendToTarget();
		logger.info("Connected to MStorm Master!");
		masterNodeClient.setChannel(ctx.getChannel());
	}

	/** Client onl send messages */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		if(e.getMessage()==null)
			return;
		Reply reply=(Reply)e.getMessage();
		int type=reply.getType();
		switch(type) {
			case Reply.CLUSTER_ID:		// Reply to Mobile Client in MStorm platform only
				Supervisor.mHandler.obtainMessage(MStorm.CLUSTER_ID, reply.getContent()).sendToTarget();
				break;
			case Reply.FAILED:	// Reply to Mobile Client in User's app only
				String failMsg = "Cannot join the MStorm cluster!";
				Supervisor.mHandler.obtainMessage(MStorm.Message_LOG, failMsg + reply.getContent()).sendToTarget();
				logger.info(failMsg);
				//masterNodeClient.close();
				break;
			case Reply.TOPOLOGY_ID: // Reply to Mobile Client in User's app only
				String topologyMsg = "Topology " + reply.getContent() + " has been scheduled!";
				logger.info(topologyMsg);
				masterNodeClient.setReply(reply);
				//masterNodeClient.close();
				break;
			case Reply.ZOOKEEPERADDR:
				logger.info("Get Zookeeper Connection String " + reply.getContent());
				MStorm.ZK_ADDRESS_IP = reply.getContent();
				break;
			/// Can be commented out for real exercise !!!!!!!!!!!!!!!
//			case Reply.GETAPK: // Reply to Mobile Client in User's app only
//				logger.info("Request apk file from master!");
//				fileClient = new FileClient(MStorm.MStormDir+"APK");
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
		// Try connecting with maximum times
		logger.debug("==== MasterNodeClient reconnectedTimes ====" + masterNodeClient.reconnectedTimes);
		masterNodeClient.reconnectedTimes++;

		super.exceptionCaught(ctx, e);
		Channel ch = ctx.getChannel();
		ch.close();

		String exceptionMSG ="Try reconnecting to MStorm master ... ";
		if(Supervisor.mHandler!=null)
			Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,exceptionMSG).sendToTarget();
		logger.info(exceptionMSG);
		Thread.sleep(masterNodeClient.TIMEOUT);

		// Try connecting with maximum times
		if(masterNodeClient.reconnectedTimes < masterNodeClient.MAX_RETRY_TIMES){
			masterNodeClient.connect();
		} else {
			String stopRetryMSG = "Have tried reconnecting to MStormMaster for " + masterNodeClient.MAX_RETRY_TIMES + " times, give up ... ";
			Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,stopRetryMSG).sendToTarget();
			logger.info(stopRetryMSG);
		}

//		// Try connecting
//		masterNodeClient.connect();
	}

	/** The channel is going to closed. */
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		/// Can be commented out for real exercise !!!!!!!!!!!!!!!
//		if(fileClient!=null) {
//			fileClient.release();   // release the resource of fileClient\
//		}
		/// Can be commented out for real exercise !!!!!!!!!!!!!!!
		super.channelClosed(ctx, e);
		Channel ch = ctx.getChannel();
		if(ch !=null && ch.getRemoteAddress()!=null) {
			String channelClosedMSG = "Disconnected to MStorm master "
					+ ((InetSocketAddress)ch.getRemoteAddress()).getAddress().getHostAddress();
			if(Supervisor.mHandler!=null)
				Supervisor.mHandler.obtainMessage(MStorm.Message_LOG,channelClosedMSG).sendToTarget();
			logger.info(channelClosedMSG);
		}
		masterNodeClient.setChannel(null);
	}
}
