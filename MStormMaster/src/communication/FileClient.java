package communication;

/**
 * Created by cmy on 5/29/16.
 */

import static org.jboss.netty.channel.Channels.pipeline;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

public class FileClient extends Thread
{
    private String apkFileDirectory;
    private ChannelFactory factory;
    private ClientBootstrap bootstrap;
    private String FileName;
    private String fileAddress;

    public FileClient(String apkDirectory){
    	apkFileDirectory = apkDirectory;
    }
	
    public void requestFile(String FileName,String fileAddress)
    {
    	this.FileName = FileName;
    	this.fileAddress = fileAddress;
    	factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory()
        {
            @Override
            public ChannelPipeline getPipeline() throws Exception
            {
                ChannelPipeline pipeline = pipeline();

                pipeline.addLast("decoder", new HttpResponseDecoder());
//              pipeline.addLast("aggregator", new HttpChunkAggregator(6048576));    // File size limit
                pipeline.addLast("encoder", new HttpRequestEncoder());
                pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
                pipeline.addLast("handler", new FileClientHandler(apkFileDirectory));

                return pipeline;
            }

        });
    }
    
    public void run(){
    	
    	System.out.println("remote ip address is :"+ fileAddress);
    	ChannelFuture future = bootstrap.connect(new InetSocketAddress(fileAddress, 8080));

        try
        {
            Thread.sleep(3000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.GET, FileName);

        future.getChannel().write(request);

        // Wait until the connection is closed or the connection attempt fails.
        future.getChannel().getCloseFuture().awaitUninterruptibly();

        // Shut down thread pools to exit.
        bootstrap.releaseExternalResources();
    }
    
/*    public void release(){
        factory.releaseExternalResources();
    }*/
}
