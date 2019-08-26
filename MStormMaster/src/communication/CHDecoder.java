/*
 * @(#) NSSAESDecoder.java Oct 24, 2012
 *
 * Copyright 2012 NHN Corp. All rights Reserved.
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package communication;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import utils.Serialization;

public class CHDecoder extends FrameDecoder {

    private static final int KB=1024;
    public static final int MAX_GOPs=1;
    private int data_rcvd=0;
	Map<ChannelHandlerContext, String> reqStrs = new HashMap<ChannelHandlerContext, String>();
   
    /**decode the received bytes into GOPs*/
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)  {    	
    	//String jsonPayload = buffer.content().toString(CharsetUtil.UTF_8);
    	
    	byte[] request = new byte[buffer.readableBytes()];
    	buffer.readBytes(request);	
    	
        String serRequest=null;
        try {
        	serRequest=new String(request, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
        
        String finalReqStr = null;
        Request req = null;
		try{
			String existingStr = reqStrs.get(ctx);
			if(existingStr == null){
				finalReqStr =  serRequest;
			} else {
				finalReqStr = existingStr + serRequest;
			}
			
			if(Serialization.isJsonValid(finalReqStr)){
				reqStrs.put(ctx, "");
				req = Serialization.Deserialize(finalReqStr, Request.class);
			}
			else {
				reqStrs.put(ctx, finalReqStr);
			}
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
		return req;       	    	
    }
}
