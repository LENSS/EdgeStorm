package com.lenss.mstorm.communication.masternode;

import android.os.AsyncTask;

import com.lenss.mstorm.core.MStorm;
import com.lenss.mstorm.utils.GNSServiceHelper;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;


public class MasterNodeClient {
    private final String TAG="MasterNodeClient";
    Logger logger = Logger.getLogger(TAG);

    private ClientBootstrap mClientBootstrap;
    private NioClientSocketChannelFactory mFactory;
    private Channel mChannel=null;
    private String mMasterNodeGUID;
    private InetSocketAddress mMasterNodeAddress;
    private Reply reply = null;

    public MasterNodeClient(String GUID) {
        mMasterNodeGUID = GUID;
        mMasterNodeAddress = new InetSocketAddress(GNSServiceHelper.getIPInUseByGUID(mMasterNodeGUID), MStorm.MASTER_PORT);
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
        mClientBootstrap.setOption("keepAlive", 10000);
        mClientBootstrap.setOption("connectTimeoutMillis", 10000);
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
        new Connect2Master().execute();
    }

    public void sendRequest(Request req) {
        if(mChannel!=null && mChannel.isWritable()) {
            mChannel.write(req);
        }
    }

    public void close() {
        if(mFactory!=null) mFactory.releaseExternalResources();
        if(mChannel!=null) mChannel.close();
    }

    public void setReply(Reply reply){
        this.reply = reply;
    }

    public Reply getReply(){
        return reply;
    }

    private class Connect2Master extends AsyncTask<Object, Channel, Channel> {
        protected Channel doInBackground(Object... objects) {
            String newMasterNodeIP = GNSServiceHelper.getIPInUseByGUID(mMasterNodeGUID);
            if(newMasterNodeIP!=null && !newMasterNodeIP.equals(mMasterNodeAddress.getAddress().getHostAddress())){
                mMasterNodeAddress = new InetSocketAddress(newMasterNodeIP, MStorm.MASTER_PORT);
            }
            ChannelFuture future = mClientBootstrap.connect(mMasterNodeAddress);
            //future.awaitUninterruptibly();
            return future.getChannel();
        }
    }
}
