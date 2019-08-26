package com.lenss.mstorm.communication.masternode;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;


public class MasterNodeClient {
    private ClientBootstrap mClientBootstrap;
    private NioClientSocketChannelFactory factory;
    private static Channel mChannel=null;


    public void setup() {
            factory = new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

            mClientBootstrap = new ClientBootstrap(factory);
            mClientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline() throws Exception {
                    return Channels.pipeline(
                            new MCHDecoder(),
                            new MasterNodeClientHandler(),
                            new MCHEncoder()
                    );
                }
            });
            mClientBootstrap.setOption("tcpNoDelay", true);
            mClientBootstrap.setOption("keepAlive", true);
            mClientBootstrap.setOption("sendBufferSize",8096);
            mClientBootstrap.setOption("receiveBufferSize",8096);
            mClientBootstrap.setOption("connectTimeoutMillis", 30000);
    }

    public static void setChannel(Channel connectedCH) {
        mChannel=connectedCH;
    }

    public boolean isConnected(){
        if(mChannel!=null){
            if(mChannel.isConnected() && mChannel.isWritable())
                return true;
            else
                return false;
        }
        else{
            return false;
        }
    }
    
    public  void connect(InetSocketAddress remoteAddress,InetSocketAddress localAddress) {    
        mClientBootstrap.connect(remoteAddress, localAddress);
    }

    public  void connect(InetSocketAddress remoteAddress) {     
        mClientBootstrap.connect(remoteAddress);
    }

    public void sendRequest(Request req) {
        if(mChannel!=null) {
            mChannel.write(req);
        }
    }

    public  void release()
    {
        if(factory!=null) factory.releaseExternalResources();
    }
}
