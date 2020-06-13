package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.utils.Serialization;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.Serializable;
import java.nio.charset.Charset;

public class CHEncoder extends SimpleChannelHandler {
	private final int INT_LENGTH = 4;

	@Override
	public void writeRequested(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
		InternodePacket pkt = (InternodePacket) event.getMessage();
		String serPkt = Serialization.Serialize(pkt);
		byte[] dataPkt = serPkt.getBytes(Charset.forName("UTF-8"));
		ChannelBuffer cb = ChannelBuffers.buffer(dataPkt.length+INT_LENGTH);
		// write the length of frame into channelBuffer
		cb.writeInt(dataPkt.length);
		// write the frame into channelBuffer
		cb.writeBytes(dataPkt);
		Channels.write(ctx, event.getFuture(), cb);
		Supervisor.mHandler.obtainMessage(MStorm.Message_GOP_SEND, dataPkt.length,1).sendToTarget();
	}
}
