package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.Supervisor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.buffer.ChannelBuffers;

public class CHEncoder extends SimpleChannelHandler {
	private int data_send=0;

	@Override
	public void writeRequested(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
			byte[] gops = (byte[]) event.getMessage();
			data_send+=gops.length;
			//Supervisor.mHandler.handleMessage(Supervisor.Message_GOP_SEND, new Integer(data_send).toString());
			ChannelBuffer cb = ChannelBuffers.buffer(gops.length);
			cb.writeBytes(gops);
			Channels.write(ctx, event.getFuture(), cb);
	}
}
