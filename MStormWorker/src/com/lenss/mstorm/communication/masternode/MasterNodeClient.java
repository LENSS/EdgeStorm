package com.lenss.mstorm.communication.masternode;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.lenss.mstorm.core.MStormWorker;
import com.lenss.mstorm.utils.GNSServiceHelper;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;


public class MasterNodeClient {
    private ClientBootstrap mClientBootstrap;
    private NioClientSocketChannelFactory factory;
    private static Channel mChannel=null;
    private String mMasterNodeGUID;
    private InetSocketAddress mMasterNodeAddress;
    private Reply reply=null;

    
    public MasterNodeClient(String GUID) {
        mMasterNodeGUID = GUID;
        setup();
    }
    
    private void setup() {
            factory = new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

            mClientBootstrap = new ClientBootstrap(factory);
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
            mClientBootstrap.setOption("keepAlive", 10000);
            mClientBootstrap.setOption("connectTimeoutMillis", 10000);
    }

    public void setChannel(Channel connectedCH) {
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
    
    public void connect() {     
    	String newMasterNodeIP = GNSServiceHelper.getIPInUseByGUID(mMasterNodeGUID);
    	InetSocketAddress mMasterNodeAddress;
    	if(newMasterNodeIP!=null){
    		mMasterNodeAddress = new InetSocketAddress(newMasterNodeIP, MStormWorker.MASTER_PORT);
    		mClientBootstrap.connect(mMasterNodeAddress);
        }
    }
    
    public void sendRequest(Request req) {
        if(mChannel!=null) {
            mChannel.write(req);
        }
    }

    public  void release()
    {
        if(factory!=null) factory.releaseExternalResources();
        if(mChannel!=null) mChannel.close();
    }
    
    public void setReply(Reply reply){
        this.reply = reply;
    }

    public Reply getReply(){
        return reply;
    }
}
