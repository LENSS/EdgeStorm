package communication;

/**
 * Created by cmy on 5/29/16.
 */
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException; 

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

public class FileClientHandler extends SimpleChannelUpstreamHandler {
	private volatile boolean readingChunks;
	private File downloadFile;
	private FileOutputStream fOutputStream = null;
	private String apkFileDirectory;
	public static boolean FileOnServer = false;

	public FileClientHandler(String apkDirectory) {
		apkFileDirectory = apkDirectory;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		// Server sends HttpResponse first, then ChunkedFiles
		if (e.getMessage() instanceof HttpResponse) {
			DefaultHttpResponse httpResponse = (DefaultHttpResponse) e
					.getMessage();
			String fileName = httpResponse.getHeader("fileName");
			downloadFile = new File(apkFileDirectory + File.separator
					+ fileName);
			if(!downloadFile.exists()){     
               try { 
            	   downloadFile.createNewFile(); 
                } catch (IOException e1) { 
                 // TODO Auto-generated catch block 
                 e1.printStackTrace(); 
                } 
			}
			readingChunks = httpResponse.isChunked();
		} else {
			HttpChunk httpChunk = (HttpChunk) e.getMessage();
			if (!httpChunk.isLast()) {
				ChannelBuffer buffer = httpChunk.getContent();
				if (fOutputStream == null) {
					fOutputStream = new FileOutputStream(downloadFile);
				}
				while (buffer.readable()) {
					byte[] dst = new byte[buffer.readableBytes()];
					buffer.readBytes(dst);
					fOutputStream.write(dst);
				}
			} else {
				readingChunks = false;
				
			}
			fOutputStream.flush();
		}
		if (!readingChunks) {
			System.out.println("Got file from mobile client to master node!");
			FileOnServer = true;
			fOutputStream.close();
			e.getChannel().close();
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		System.out.println("here i am");
		System.out.println(e.getCause());
	}
}