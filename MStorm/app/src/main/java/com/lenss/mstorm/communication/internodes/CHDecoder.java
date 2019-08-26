package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.core.Supervisor;
import com.lenss.mstorm.utils.Serialization;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.io.UnsupportedEncodingException;

public class CHDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
        // Make sure if the length field was received.
        if (buffer.readableBytes() < 4) {
            return null;
        }

        // The length field is in the buffer.

        // Mark the current buffer position before reading the length field
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buffer.markReaderIndex();

        // Read the length field.
        int length = buffer.readInt();

        // Make sure if there's enough bytes in the buffer.
        if (buffer.readableBytes() < length) {
            // The whole bytes were not received yet - return null.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.
            // Reset to the marked position to read the length field again
            // next time.
            buffer.resetReaderIndex();
            return null;
        }

        // There's enough bytes in the buffer. Read it.
        byte[] dataPkt = new byte[length];
        buffer.readBytes(dataPkt);

        String strPkt = null;
        try {
            strPkt = new String(dataPkt, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        InternodePacket pkt = null;
        if (strPkt != null){
            pkt = (InternodePacket) Serialization.Deserialize(strPkt, InternodePacket.class);
        }

        Supervisor.mHandler.obtainMessage(MStorm.Message_GOP_RECVD, length,1).sendToTarget();

        // Successfully decoded a frame.  Return the decoded frame.
        return pkt;
    }
}
