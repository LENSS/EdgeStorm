package com.lenss.mstorm.communication.masternode;

import com.lenss.mstorm.utils.Serialization;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import java.nio.charset.Charset;
import static org.jboss.netty.buffer.ChannelBuffers.buffer;


public class MCHEncoder extends SimpleChannelHandler {
	private final String TAG="MasterNode.MCHEncoder";
	Logger logger = Logger.getLogger(TAG);

	@Override
	public void writeRequested(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
		Request req = (Request) event.getMessage();
		if(req!=null) {
			String serTopology = Serialization.Serialize(req);
			byte[] data = serTopology.getBytes(Charset.forName("UTF-8"));
			ChannelBuffer cb = buffer(data.length);
			cb.writeBytes(data);
			Channels.write(ctx, event.getFuture(), cb);
			logger.info("==== Something sent to MStorm master ====");
		}
	}
}
