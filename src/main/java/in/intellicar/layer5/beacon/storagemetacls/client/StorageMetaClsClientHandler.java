package in.intellicar.layer5.beacon.storagemetacls.client;

import in.intellicar.layer5.beacon.Layer5Beacon;
import in.intellicar.layer5.beacon.Layer5BeaconDeserializer;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.account.AccountRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.account.AccountRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.account.NamespaceRegReq;
import in.intellicar.layer5.beacon.storagemetacls.account.NamespaceRegRsp;
import in.intellicar.layer5.beacon.storagemetacls.instance.InstanceRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.instance.InstanceRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.instance.StorageClsMetaErrorRsp;
import in.intellicar.layer5.data.Deserialized;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * <h1>Inbound handler</h1>
 * processes inbound data
 *
 * @author  krishna mohan
 * @version 1.0
 * @since   2021-02-18
 */
public class StorageMetaClsClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private Logger _logger;
    private Layer5BeaconParser _l5parser;
    private byte[] _handlerBuffer;
    private int _bufridx;
    private int _bufwidx;
    private String _serverName;
    private static int _nameIncrementer = 0;
    //private ByteBuf _byteBuf;
    /**
     * Constructor
     */
    public StorageMetaClsClientHandler(Layer5BeaconParser lL5Parser, String lServerName,Logger lLogger)
    {
        _logger = lLogger;
        _l5parser = lL5Parser;
        _serverName=  lServerName;

        Layer5BeaconDeserializer storageMetaClsAPIDeser = new StorageClsMetaBeaconDeser();
        _l5parser.registerDeserializer(storageMetaClsAPIDeser.getBeaconType(), storageMetaClsAPIDeser);

        _handlerBuffer = new byte[16 * 1024];
        _bufridx = 0;
        _bufwidx = 0;
    }

    @Override
//    public void channelActive(ChannelHandlerContext ctx) throws Exception
//    {

//    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        //instanceRegisterReq
        byte[] ipBytes = {(byte)0xC0, (byte)0xA8, (byte)0x49, (byte)0x96};
        InstanceRegisterReq registerReqData = new InstanceRegisterReq(_serverName, 9999, ipBytes);
        StorageClsMetaBeacon metaBeacon = new StorageClsMetaBeacon(223, registerReqData);

//        //AccountRegisterReq
//        AccountRegisterReq accRegReq = new AccountRegisterReq("in.intellicar" + _nameIncrementer++);
//        StorageClsMetaBeacon accRegReqBeacon = new StorageClsMetaBeacon(223, accRegReq);

        //NamespaceRegReq
//        byte[] accIdBytes = LittleEndianUtils.hexStringToByteArray("1BA5EC591BB03A6F4DF248BE577CA904A81A9BBAE8678B65C59EB08B8636A24C");
//        NamespaceRegReq nsRegReq = new NamespaceRegReq("ns"+ _nameIncrementer++, new SHA256Item(accIdBytes));
//        StorageClsMetaBeacon nsRegReqBeacon = new StorageClsMetaBeacon(233, nsRegReq);
        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(metaBeacon)));
    }

    private byte[] returnSerializedByteStreamOfBeacon (StorageClsMetaBeacon lBeacon)
    {
        int beaconSize = lBeacon.getBeaconSize();
        byte[] beaconSerializedBuffer = new byte[beaconSize];
        int metaBeaconSerializedBufferIndex = _l5parser.serialize(beaconSerializedBuffer, 0, beaconSize, lBeacon, _logger);
        return beaconSerializedBuffer;
    }

    private void adjustBuffer() {
        _logger.info("Adjusting buffer: " + _bufridx + " , " + _bufwidx);
        if (_bufwidx - _bufridx > 0 && _bufridx > 0) {
            System.arraycopy(_handlerBuffer, _bufridx, _handlerBuffer, 0, _bufwidx - _bufridx);
            _bufwidx -= _bufridx;
            _bufridx = 0;
        }
    }

    private void skipBytesTillHeader() {
        for (int i = _bufridx; i < _bufwidx; i++) {
//            if (!l5parser.isHeaderAvailable(handlerBuffer, bufridx, bufwidx, logger)){
//                break;
//            }
            if (_l5parser.isHeaderValid(_handlerBuffer, _bufridx, _bufwidx, _logger)) {
                break;
            }
            _bufridx++;
        }
        adjustBuffer();
    }

    private ArrayList<StorageClsMetaBeacon> parseLayer5Beacons() {
        ArrayList<StorageClsMetaBeacon> beaconsFound = new ArrayList();
        while (_l5parser.isHeaderAvailable(_handlerBuffer, _bufridx, _bufwidx, _logger)) {
            if (!_l5parser.isHeaderValid(_handlerBuffer, _bufridx, _bufwidx, _logger)) {
                skipBytesTillHeader();
                continue;
            }
            if (!_l5parser.isDataSufficient(_handlerBuffer, _bufridx, _bufwidx, _logger)) {
                break;
            }
            Deserialized<Layer5Beacon> layer5BeaconD = _l5parser.deserialize(_handlerBuffer, _bufridx, _bufwidx, _logger);
            if (layer5BeaconD.data != null)
            {
                Layer5Beacon beacon = layer5BeaconD.data;
//                Append only StorageMetaClsBeacons
                if (beacon.getBeaconType() == 1 ) {
                    beaconsFound.add((StorageClsMetaBeacon) beacon);
                }
//                Ignore other beacons
                _bufridx = layer5BeaconD.curridx;
                adjustBuffer();
            }
            else // Either deserializer not found or crc check failed. Here we are just breaking the loop.
            // Better to skip the beacon in _l5parser.deserializer
            {
                break;
            }
        }
        return beaconsFound;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception
    {
        //_logger.info("Client received message:" + msg.toString(CharsetUtil.UTF_8));
        //read response
        ByteBuf byteBuf = (ByteBuf) msg;
        _logger.info("Received a message");

        ArrayList<StorageClsMetaBeacon> beaconsFound = new ArrayList<>();

        while (byteBuf.isReadable() && _bufwidx < (_handlerBuffer.length - 1024))
        {
            int bytesToCopy = 1024;
            if (byteBuf.readableBytes() < 1024) {
                bytesToCopy = byteBuf.readableBytes();
            }
            byteBuf.readBytes(_handlerBuffer, _bufwidx, bytesToCopy);
            _bufwidx += bytesToCopy;
            beaconsFound.addAll(parseLayer5Beacons());
        }

        _logger.info("Total beacons Found: " + beaconsFound.size());
        // Handle StorageMetaClsBeacons
        for (StorageClsMetaBeacon beacon : beaconsFound)
        {
            StorageClsMetaPayload payload = beacon.payload;
            short subType = payload.getSubType();
            PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

            switch (payloadType)
            {
                case INSTANCE_REGISTER_RSP:
                    _logger.info("Received Instance Register Response");
                    InstanceRegisterRsp registerRsp = (InstanceRegisterRsp) payload;
                    byte[] instanceIdBuff = new byte[registerRsp.instanceID.hashdata.length];
                    //_logger.info("Beacon Serialized, InstanceId: " + LittleEndianUtils.printHexArray(registerRsp.instanceID.hashdata,
                    //        0, registerRsp.instanceID.hashdata.length));
                    _logger.info(registerRsp.toJsonString(_logger));
//                    TODO:: send bucket request
                    break;
                case INSTANCE_BUCKET_RSP:
                    _logger.info("Received Instance Bucket Response");
                    break;
//                    TODO:: send role request
                case ACCOUNT_REGISTER_RSP:
                    _logger.info("Account Register Response");
                    AccountRegisterRsp accRegRsp = (AccountRegisterRsp) payload;
                    byte[] AccIdInBuffer = new byte[accRegRsp.accountID.hashdata.length];
                    _logger.info(accRegRsp.toJsonString(_logger));
                    break;
                case NAMESPACE_REGISTER_RSP:
                    _logger.info("NameSpace Register Response");
                    NamespaceRegRsp nsRegRsp = (NamespaceRegRsp) payload;
                    byte[] nsIdInBuffer = new byte[nsRegRsp.namespaceID.hashdata.length];
                    _logger.info(nsRegRsp.toJsonString(_logger));
                    break;

                case STORAGE_CLS_META_ERROR_RSP:
                    StorageClsMetaErrorRsp rspError = (StorageClsMetaErrorRsp) payload;
                    _logger.info("Received error" + new String(rspError.errorCauseInUtf8, StandardCharsets.UTF_8));
                    break;
                default:
                    _logger.info("Received Generic Response");
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        cause.printStackTrace();
        ctx.close();
    }
}
