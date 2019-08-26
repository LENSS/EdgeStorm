package com.lenss.mstorm.communication.masternode;

/**
 * ClassName: FileServer
 * Function:  Send apk file to mStorm master node (Nimbus)
 */

import static org.jboss.netty.channel.Channels.pipeline;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

public class FileServer {
    private String apkFileDirectory;
    private ChannelFactory factory;
    private ServerBootstrap bootstrap;
    private final int HTTP_PORT = 8080;

    public FileServer(String apkDirectory) {
        apkFileDirectory = apkDirectory;
    }

    public void setup() {
        factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = pipeline();
                pipeline.addLast("decoder", new HttpRequestDecoder());
//              pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
                pipeline.addLast("encoder", new HttpResponseEncoder());
                pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
                pipeline.addLast("handler", new FileServerHandler(apkFileDirectory));
                return pipeline;
            }
        });
        bootstrap.bind(new InetSocketAddress(HTTP_PORT));
    }

    public void release() {
        if(factory!=null)
            factory.releaseExternalResources();
    }

}