package in.intellicar.layer5.beacon.storagemetacls.client;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.logging.Logger;

/**
 * <h1>StorageMetaClusterAPI Client!</h1>
 * Client class, used to start the client
 *
 * @author  krishna mohan
 * @version 1.0
 * @since   2021-02-18
 */
public class StorageMetaClsClient implements Runnable
{
    private Logger _logger;
    private final String _host;
    private final int _port;
    private ChannelFuture _channelFuture;
    private Thread _clientThread;
    private String _serverName;

    /**
     * Constructor
     * @param lHost server address
     * @param lPort  server port
     */
    public StorageMetaClsClient(String lHost, int lPort, String lServerName, Logger lLogger) {
        _host = lHost;
        _port = lPort;
        _logger = lLogger;
        _channelFuture = null;
        _clientThread = null;
        _serverName= lServerName;
    }

    public void startClient()
    {
        _clientThread = new Thread(this);
        _clientThread.start();
    }

    public void stopClient() throws InterruptedException {
        //_channelFuture.channel().close(); //need to check if this close helps to close the connection or not
        _clientThread.join();
    }

    @Override
    public void run()
    {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new StorageMetaClsClientInitializer(_serverName, _logger));

            _channelFuture = b.connect(_host, _port).sync();

            _channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }

    }

    public static void main(String[] args) throws Exception
    {
        Logger logger = Logger.getLogger("Client");

        //many instances for testing
        StorageMetaClsClient[] clients = new StorageMetaClsClient[15];
        for (int i = 0; i < 15 ; i++)
        {
            clients[i] = new StorageMetaClsClient("192.168.73.150", 10107, "Server" + i, logger);
            clients[i].startClient();
        }
        for(int i = 0; i < 15; i++)
        {
            clients[i].stopClient();
        }

        //////Single client
//        StorageMetaClsClient client = new StorageMetaClsClient("192.168.73.150", 10107, "Server4Sync", logger);
//        //client.startClient();
//        Thread clientThread = new Thread(client);
//        clientThread.start();
//        clientThread.join();

    }
}
