package com.lenss.mstorm.communication.internodes;
import com.lenss.mstorm.utils.GNSServiceHelper;
import org.jboss.netty.bootstrap.ClientBootstrap;
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
    private ClientBootstrap mClientBootstrap;
    private NioClientSocketChannelFactory factory;
    public final int TIMEOUT = 5000;
    public void setup() {
        factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        mClientBootstrap = new ClientBootstrap(factory);
        mClientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                        new CHDecoder(),
                        new CommunicationClientHandler(CommunicationClient.this),
                        new CHEncoder()
                );
            }
        });
        mClientBootstrap.setOption("tcpNoDelay", true);
        mClientBootstrap.setOption("keepAlive", true);
        mClientBootstrap.setOption("keepAlive", 10000);
        mClientBootstrap.setOption("connectTimeoutMillis", TIMEOUT);
    }

    public ChannelFuture connectByGUID(String remoteGUID){
        String remoteIPInUse = GNSServiceHelper.getIPInUseByGUID(remoteGUID);
        ChannelFuture cf = connectByIP(remoteIPInUse);
        return cf;
    }

    public ChannelFuture connectByIP(String remoteIP){
        ChannelFuture cf = mClientBootstrap.connect(new InetSocketAddress(remoteIP, CommunicationServer.SERVER_PORT));
        return cf;
    }

    public void release() {
        if(factory!=null)
            factory.releaseExternalResources();
    }

}

