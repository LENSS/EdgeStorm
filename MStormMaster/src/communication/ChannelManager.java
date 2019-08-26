package communication;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import java.net.SocketAddress;
import java.util.ArrayList;


public class ChannelManager {
    private static final ChannelGroup RecChannels= new DefaultChannelGroup("MyServer");
    private static final ChannelGroup SendChannels=new DefaultChannelGroup("Clients");
    public static void addRec(Channel ch)
    {
        RecChannels.add(ch);
    }
    public static final ArrayList<Integer> chIds=new ArrayList<Integer>();
    public static void release()
    {
        RecChannels.close().awaitUninterruptibly();
        SendChannels.close().awaitUninterruptibly();

    }

    public static void addSend(Channel ch)
    {
        synchronized (chIds) {
            String local = ch.getLocalAddress().toString();
            int chId = ch.getId();
            chIds.add(chId);
       /*if(!local.equals("/"+GReporter.getLocalAddress()+"2015")) {
            int taskid = ComputingNode.mappings.getNode2Task().get(local.substring(1,local.length()));
            Dispatcher.putChannel(taskid,chId);
        }*/
            SendChannels.add(ch);
        }
    }
    public static synchronized boolean empty(){
        return SendChannels.isEmpty();
    }
    public static void broadcast(byte[] gop)
    {
        SendChannels.write(gop);
    }
    public static int size()
    {
        return SendChannels.size();
    }
    public static void write(byte[] gop,SocketAddress address)
    {
        SendChannels.write(gop,address);

    }
    public static void write(byte[] gop,Integer id)
    {
        SendChannels.find(id).write(gop);
    }
}
