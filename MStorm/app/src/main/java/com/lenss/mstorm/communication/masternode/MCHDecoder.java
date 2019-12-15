package com.lenss.mstorm.communication.masternode;

import com.lenss.mstorm.utils.Serialization;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import java.io.UnsupportedEncodingException;

public class MCHDecoder extends FrameDecoder {
    private final String TAG="MasterNode.MCHDecoder";
    Logger logger = Logger.getLogger(TAG);

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
        try {
            byte[] reply = new byte[buffer.readableBytes()];
            buffer.readBytes(reply);
            logger.info("==== Something received from MStorm master ====");
            String serReply = new String(reply, "UTF-8");
            if (serReply != null) {
                Object recReply = Serialization.Deserialize(serReply, Reply.class);
                return recReply;
            } else {
                return null;
            }
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
}
