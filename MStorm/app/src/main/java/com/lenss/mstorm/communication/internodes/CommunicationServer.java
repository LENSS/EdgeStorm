package com.lenss.mstorm.communication.internodes;

import com.lenss.mstorm.core.ComputingNode;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** each computing node owns a server with a fixed port,
 *  and have different sending clients with different ports
 */
public class CommunicationServer  {

    public static final int SERVER_PORT = 12015;

    private ChannelFactory factory;
    private ServerBootstrap bootstrap;

    public void setup() {
        factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                        new CHDecoder(),
                        new CommunicationServerHandler(),
                        new CHEncoder());
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.bind(new InetSocketAddress(SERVER_PORT));
    }

    public void release() {
        if(factory!=null)
            factory.releaseExternalResources();
    }
}
