package com.lenss.mstorm.communication.internodes;


import com.lenss.mstorm.core.ComputingNode;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** each computing node owns a server with a fixed port,
 *  and have different clients with different ports */

public class CommunicationClient  {
    private  ClientBootstrap mClientBootstrap;
    private  NioClientSocketChannelFactory factory;

    public void setup() {
        factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        mClientBootstrap = new ClientBootstrap(factory);
        mClientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                        new CHDecoder(),
                        new CommunicationClientHandler(),
                        new CHEncoder()
                );
            }
        });
        mClientBootstrap.setOption("tcpNoDelay", true);
        mClientBootstrap.setOption("keepAlive", true);
        mClientBootstrap.setOption("connectTimeoutMillis", 30000);
    }

    public  void connect(InetSocketAddress remoteAddress,InetSocketAddress localAddress) {
        mClientBootstrap.connect(remoteAddress, localAddress);
    }

    public  void connect(InetSocketAddress remoteAddress) {
        mClientBootstrap.connect(remoteAddress);
    }

    public  void release() {
        if(factory!=null)
            factory.releaseExternalResources();
    }

}
