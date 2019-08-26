package com.lenss.mstorm.communication.internodes;


import com.lenss.mstorm.core.Supervisor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class CHDecoder extends FrameDecoder {
    private int data_rcvd=0;
    private byte[] gop;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
        data_rcvd+=buffer.readableBytes();
        //Supervisor.mHandler.handleMessage(Supervisor.Message_GOP_RECVD,new Integer(data_rcvd).toString());
        gop=new byte[buffer.readableBytes()];
        buffer.readBytes(gop);
        return gop;
    }
}
