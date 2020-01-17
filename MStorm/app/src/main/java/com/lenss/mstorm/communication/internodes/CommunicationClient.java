package com.lenss.mstorm.communication.internodes;
import android.telecom.Call;

import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.utils.GNSServiceHelper;
import com.lenss.mstorm.utils.Helper;
import com.lenss.mstorm.core.ComputingNode;

import org.apache.zookeeper.server.quorum.CommitProcessor;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** each computing node owns a server with a fixed port,
 *  and have different clients with different ports */

public class CommunicationClient  {
    private ClientBootstrap mClientBootstrap;
    private NioClientSocketChannelFactory factory;
    public final int TIMEOUT = 10000;

    // Try connecting with maximum times
    public int reconnectedTimes = 0;
    public static int MAX_RETRY_TIMES = 200;

    ExecutorService executorService;

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
        mClientBootstrap.setOption("connectTimeoutMillis", TIMEOUT);
    }

    public ChannelFuture connectByGUID(String remoteGUID){
        String remoteIPInUse = GNSServiceHelper.getIPInUseByGUID(remoteGUID);
        ChannelFuture cf = connectByIP(remoteIPInUse);
        return cf;
    }

//    public ChannelFuture connectByIP(String remoteIP){
//        ChannelFuture cf = mClientBootstrap.connect(new InetSocketAddress(remoteIP, CommunicationServer.SERVER_PORT));
//        return cf;
//    }

    public ChannelFuture connectByIP(String remoteIP){
        ChannelFuture cf = null;
        executorService = Executors.newSingleThreadExecutor();
        Future<ChannelFuture> future = executorService.submit(new Callable<ChannelFuture> (){
            @Override
            public ChannelFuture call() throws Exception{
                return mClientBootstrap.connect(new InetSocketAddress(remoteIP, CommunicationServer.SERVER_PORT));
            }
        });
        try{
            cf = future.get();
        } catch(Exception e){
            System.out.println("== Cannot connect to remoteIP ==" + remoteIP);
        }
        return cf;
    }

    public void release() {
        if(factory!=null)
            factory.releaseExternalResources();
    }

}

