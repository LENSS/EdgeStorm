
package communication;


import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import utils.Serialization;
import static org.jboss.netty.buffer.ChannelBuffers.buffer;


public class CHEncoder extends SimpleChannelHandler {



	@Override
	public void writeRequested(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
		Reply reply=(Reply) event.getMessage();
		if(reply != null){
			String serReply=Serialization.Serialize(reply);
			byte[] bytes =serReply.getBytes(Charset.forName("UTF-8"));
			ChannelBuffer cb = buffer(bytes.length);
			cb.writeBytes(bytes);
			Channels.write(ctx, event.getFuture(), cb);
		}
	}
}
