package in.intellicar.layer5.beacon.storagemetacls.client;

import in.intellicar.layer5.beacon.Layer5BeaconParser;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.util.logging.Logger;

/**
 * <h1>StorageMetaClusterAPI Client initializer</h1>
 * Initializes SocketChannel
 *
 * @author  krishna mohan
 * @version 1.0
 * @since   2021-02-18
 */
public class StorageMetaClsClientInitializer extends ChannelInitializer<SocketChannel> {

    private Logger _logger;
    private String _serverName;
    /**
     * Constructor
     */
    public StorageMetaClsClientInitializer(String lServerName, Logger lLogger)
    {
        _logger = lLogger;
        _serverName=  lServerName;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast("client handler", new StorageMetaClsClientHandler(Layer5BeaconParser.getHandler("ClientInit", _logger),
                _serverName, _logger));
    }
}
