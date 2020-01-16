package communication;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/** server with a fixed port, different sending clients with different port number*/
public class CommunicationServer  {
    static final int SERVER_PORT = 12016;
    static final int workerThreads = 10;
    
    Logger logger = Logger.getLogger("CommunicationServer");

    private ChannelFactory factory;

    public CommunicationServer() { 	
		// TODO Auto-generated constructor stub
	}
	public void setup() {
            factory = new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            ServerBootstrap bootstrap = new ServerBootstrap(factory);
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
            bootstrap.setOption("child.keepAlive", 10000);
            bootstrap.bind(new InetSocketAddress(SERVER_PORT));
                        
            List<String> ipAddresses = new ArrayList<String>();
            ipAddresses.add("localhost");
            
//            try {
//            	gnsHandler = new GNSHandler("distressnet-mstorm");
//				gnsHandler.registerToGNS();
//				//gnsHandler.deRegisterGNS();
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
    }
    public void release()
    {
        factory.releaseExternalResources();
    }
}
