package com.lenss.mstorm.communication.masternode;

import java.io.File;
import java.io.FileOutputStream;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

public class FileClientHandler extends SimpleChannelUpstreamHandler {
    private volatile boolean readingChunks;
    private File downloadFile;
    private FileOutputStream fOutputStream = null;
    private String apkDirectory;
    public static boolean FileOnMachine = false;

    public FileClientHandler(String apkDir){
        apkDirectory = apkDir;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        // Server sends HttpResponse first, then chunk files
        if (e.getMessage() instanceof HttpResponse) {
            DefaultHttpResponse httpResponse = (DefaultHttpResponse) e.getMessage();
            String fileName = httpResponse.getHeader("fileName");
            downloadFile = new File(apkDirectory + File.separator + fileName);
            readingChunks = httpResponse.isChunked();
        } else {
            HttpChunk httpChunk = (HttpChunk) e.getMessage();
            if (!httpChunk.isLast()) {
                ChannelBuffer buffer = httpChunk.getContent();
                if (fOutputStream == null) {
                    fOutputStream = new FileOutputStream(downloadFile);
                }
                while (buffer.readable()) {
                    byte[] dst = new byte[buffer.readableBytes()];
                    buffer.readBytes(dst);
                    fOutputStream.write(dst);
                }
            } else {
                readingChunks = false;
            }
            fOutputStream.flush();
        }
        if (!readingChunks) {
            FileOnMachine = true;
            fOutputStream.close();
            e.getChannel().close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        super.exceptionCaught(ctx, e);
        if(ctx.getChannel() != null && ctx.getChannel().isOpen()) {
            ctx.getChannel().close();
        }
    }
}