package com.lenss.mstorm.communication.masternode;


import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.utils.GNSServiceHelper;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MasterNodeClient {
    private final String TAG="MasterNodeClient";
    Logger logger = Logger.getLogger(TAG);

    private ClientBootstrap mClientBootstrap;
    private NioClientSocketChannelFactory mFactory;
    private Channel mChannel=null;
    private String mMasterNodeGUID;
    private String mMasterNodeIP;
    private Reply reply = null;
    public final int TIMEOUT = 10000;

    // Try connecting with maximum times
    public int reconnectedTimes = 0;
    public static int MAX_RETRY_TIMES = 200;

    ExecutorService executorService;

    public MasterNodeClient(String GUID) {
        mMasterNodeGUID = GUID;
        createClientBootstrap();
    }

    private void createClientBootstrap() {
        mFactory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

        mClientBootstrap = new ClientBootstrap(mFactory);
        mClientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                        new MCHDecoder(),
                        new MasterNodeClientHandler(MasterNodeClient.this),
                        new MCHEncoder()
                );
            }
        });
        mClientBootstrap.setOption("tcpNoDelay", true);
        mClientBootstrap.setOption("keepAlive", true);
        mClientBootstrap.setOption("connectTimeoutMillis", TIMEOUT);
    }

    public void setChannel(Channel connectedCH) {
        mChannel=connectedCH;
    }

    public Channel getChannel(){return mChannel;}

    public boolean isConnected(){
        if(mChannel!=null && mChannel.isConnected()){
            return true;
        }
        else{
            return false;
        }
    }

    public void connect() {
        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable(){
            @Override
            public void run(){
                String newMasterNodeIP = GNSServiceHelper.getIPInUseByGUID(mMasterNodeGUID);
                // update MasterNodeIP
                if(newMasterNodeIP!=null){
                    mMasterNodeIP = newMasterNodeIP;
                }
                InetSocketAddress mMasterNodeAddress;
                if(mMasterNodeIP!=null) {
                    mMasterNodeAddress = new InetSocketAddress(mMasterNodeIP, MStorm.MASTER_PORT);
                    mClientBootstrap.connect(mMasterNodeAddress);
                }
            }
        });
    }

    public void sendRequest(Request req) {
        if(mChannel!=null && mChannel.isWritable()) {
            mChannel.write(req);
        }
    }

    public void close() {
        if(mFactory!=null) mFactory.releaseExternalResources();
        if(mChannel!=null) mChannel.close();
        executorService.shutdownNow();
    }

    public void setReply(Reply reply){
        this.reply = reply;
    }

    public Reply getReply(){
        return reply;
    }
}
