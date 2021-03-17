package in.intellicar.layer5.beacon.storagemetacls.client;

import in.intellicar.layer5.beacon.Layer5Beacon;
import in.intellicar.layer5.beacon.Layer5BeaconDeserializer;
import in.intellicar.layer5.beacon.Layer5BeaconParser;
import in.intellicar.layer5.beacon.storagemetacls.PayloadTypes;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeacon;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaBeaconDeser;
import in.intellicar.layer5.beacon.storagemetacls.StorageClsMetaPayload;
import in.intellicar.layer5.beacon.storagemetacls.payload.*;
import in.intellicar.layer5.beacon.storagemetacls.payload.metaclsservice.*;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.client.*;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.AccIdRegisterRsp;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterReq;
import in.intellicar.layer5.beacon.storagemetacls.payload.namenodeservice.internal.NsIdRegisterRsp;
import in.intellicar.layer5.data.Deserialized;
import in.intellicar.layer5.utils.LittleEndianUtils;
import in.intellicar.layer5.utils.sha.SHA256Item;
import in.intellicar.layer5.utils.sha.SHA256Utils;
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
    public StorageMetaClsClientHandler(Layer5BeaconParser lL5Parser, String lServerName, Logger lLogger) {
        _logger = lLogger;
        _l5parser = lL5Parser;
        _serverName = lServerName;

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

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //instanceRegisterReq
//        byte[] ipBytes = {(byte)0xC0, (byte)0xA8, (byte)0x00, (byte)0x70};
//        InstanceRegisterReq registerReqData = new InstanceRegisterReq(_serverName, 9999, ipBytes);
//        StorageClsMetaBeacon metaBeacon = new StorageClsMetaBeacon(223, registerReqData);
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(metaBeacon)));

        // InstanceIdToBuckReq
//        _logger.info("Sending InstanceIdToBuckReq");
//        InstanceIdToBuckReq instanceIdToBuckReq = new InstanceIdToBuckReq(new SHA256Item(LittleEndianUtils.hexStringToByteArray("210291032E56B4886BDE2A6EF6311983C94AE460E61B6453DA5BA859EF76ACD8")));
//        StorageClsMetaBeacon inToBuckReqBeacon = new StorageClsMetaBeacon(223, instanceIdToBuckReq);
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(inToBuckReqBeacon)));

        //AccIDMetaInstanceRequest
//        AccIdMetaInstanceReq accountInstanceReqReq = new AccIdMetaInstanceReq(SHA256Utils.getSHA256("in.intellicar"));
//        StorageClsMetaBeacon accInstReqBeacon = new StorageClsMetaBeacon(223, accountInstanceReqReq);
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(accInstReqBeacon)));

        // AccIdGenerateReq
//        AccIdGenerateReq accIdGenerateReq = new AccIdGenerateReq("in.intellicar".getBytes());
//        StorageClsMetaBeacon accIDReqBeacon = new StorageClsMetaBeacon(223, accIdGenerateReq);
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(accIDReqBeacon)));

        // AccIdRegisterReq
//        SHA256Item accID = new SHA256Item(LittleEndianUtils.hexStringToByteArray("55557ABFA4987C9BBE8D29085C26F046C80FB422C7A09E5587240145BB0E928F"));
//        AccIdRegisterReq accIdRegisterReq = new AccIdRegisterReq(accID, "in.intellicar".getBytes(StandardCharsets.UTF_8), "1000a6e5df65".getBytes());
//        StorageClsMetaBeacon accRegReqBeacon = new StorageClsMetaBeacon(233, accIdRegisterReq);
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(accRegReqBeacon)));

        // NsIdGenerateReq
//        SHA256Item accID = new SHA256Item(LittleEndianUtils.hexStringToByteArray("55557ABFA4987C9BBE8D29085C26F046C80FB422C7A09E5587240145BB0E928F"));
//        NsIdGenerateReq nsIdGenerateReq = new NsIdGenerateReq("analytics".getBytes(), accID);
//        StorageClsMetaBeacon nsIDReqBeacon = new StorageClsMetaBeacon(223, nsIdGenerateReq);
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(nsIDReqBeacon)));

        // NsIdRegisterReq
//            SHA256Item accID = new SHA256Item(LittleEndianUtils.hexStringToByteArray("55557ABFA4987C9BBE8D29085C26F046C80FB422C7A09E5587240145BB0E928F"));
            SHA256Item nsID = new SHA256Item(LittleEndianUtils.hexStringToByteArray("865FB03325031E90F653617D7C32CEFCA401DC60A971FA695DE51864A208E172"));
//            NsIdRegisterReq nsIdRegisterReq = new NsIdRegisterReq(nsID, accID, "analytics".getBytes(StandardCharsets.UTF_8), "15ac608b288f".getBytes());
//            StorageClsMetaBeacon nsIDReqBeacon = new StorageClsMetaBeacon(223, nsIdRegisterReq);
//            _logger.info(nsIdRegisterReq.toJsonString(_logger));
//            ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(nsIDReqBeacon)));

        //DirIdGenerateAndRegisterReq without parentDir
//        DirIdGenerateAndRegisterReq dirIdGenerateAndRegisterReq = new DirIdGenerateAndRegisterReq(
//                nsID, null, "naveen".getBytes(StandardCharsets.UTF_8));
//        StorageClsMetaBeacon dirIDReqBeacon = new StorageClsMetaBeacon(223, dirIdGenerateAndRegisterReq);
//        _logger.info(dirIdGenerateAndRegisterReq.toJsonString(_logger));
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(dirIDReqBeacon)));

        //DirIdGenerateAndRegisterReq with parentDir
        SHA256Item parentDirID =new SHA256Item(LittleEndianUtils.hexStringToByteArray("68E6BB9A04A10974CF26354CE250116746E2CC82BBB95ED118CA8B05034FB0BB"));
//        DirIdGenerateAndRegisterReq dirIdGenerateAndRegisterReq = new DirIdGenerateAndRegisterReq(
//                nsID, parentDirID, "newplatform".getBytes(StandardCharsets.UTF_8));
//        StorageClsMetaBeacon dirIDReqBeacon = new StorageClsMetaBeacon(223, dirIdGenerateAndRegisterReq);
//        _logger.info(dirIdGenerateAndRegisterReq.toJsonString(_logger));
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(dirIDReqBeacon)));

        // FileIdGenerateAndRegisterReq
//        FileIdGenerateAndRegisterReq fileIdGenerateAndRegisterReq = new FileIdGenerateAndRegisterReq(nsID, parentDirID, "test.csv".getBytes(StandardCharsets.UTF_8));
//        StorageClsMetaBeacon fileGenerateAndRegisterBeacon = new StorageClsMetaBeacon(223, fileIdGenerateAndRegisterReq);
//        _logger.info(fileGenerateAndRegisterBeacon.toJsonString(_logger));
//        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(fileGenerateAndRegisterBeacon)));

        // FileVersionIdGenerateAndRegisterReq
        SHA256Item fileID = new SHA256Item(LittleEndianUtils.hexStringToByteArray("5ADF3797F2089AD80F54699178DEF7B9F16FE9DB57F67066F6EB910AC3C80AB7"));
        FileVersionIdGenerateAndRegisterReq fileVersionIdGenerateAndRegisterReq = new FileVersionIdGenerateAndRegisterReq(fileID);
        StorageClsMetaBeacon fileVersionBeacon = new StorageClsMetaBeacon(223, fileVersionIdGenerateAndRegisterReq);
        _logger.info(fileVersionBeacon.toJsonString(_logger));
        ctx.writeAndFlush(Unpooled.wrappedBuffer(returnSerializedByteStreamOfBeacon(fileVersionBeacon)));
    }

    private byte[] returnSerializedByteStreamOfBeacon(StorageClsMetaBeacon lBeacon) {
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
            if (layer5BeaconD.data != null) {
                Layer5Beacon beacon = layer5BeaconD.data;
//                Append only StorageMetaClsBeacons
                if (beacon.getBeaconType() == 1) {
                    beaconsFound.add((StorageClsMetaBeacon) beacon);
                }
//                Ignore other beacons
                _bufridx = layer5BeaconD.curridx;
                adjustBuffer();
            } else // Either deserializer not found or crc check failed. Here we are just breaking the loop.
            // Better to skip the beacon in _l5parser.deserializer
            {
                break;
            }
        }
        return beaconsFound;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        //_logger.info("Client received message:" + msg.toString(CharsetUtil.UTF_8));
        //read response
        ByteBuf byteBuf = (ByteBuf) msg;
        _logger.info("Received a message");

        ArrayList<StorageClsMetaBeacon> beaconsFound = new ArrayList<>();

        while (byteBuf.isReadable() && _bufwidx < (_handlerBuffer.length - 1024)) {
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
        for (StorageClsMetaBeacon beacon : beaconsFound) {
            StorageClsMetaPayload payload = beacon.payload;
            short subType = payload.getSubType();
            PayloadTypes payloadType = PayloadTypes.getPayloadType(subType);

            switch (payloadType) {
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
                    InstanceIdToBuckRsp rsp = (InstanceIdToBuckRsp) payload;
                    _logger.info(rsp.toJsonString(_logger));
                    break;

                case ACCOUNT_ID_GEN_RSP:
                    _logger.info("Received AccountID Generate Response");
                    AccIdGenerateRsp accIdGenerateRsp = (AccIdGenerateRsp) payload;
                    _logger.info(accIdGenerateRsp.toJsonString(_logger));
                    break;
                case ACCOUNT_ID_REG_RSP:
                    _logger.info("Received AccountID Register Response");
                    AccIdRegisterRsp accIdRegisterRsp = (AccIdRegisterRsp) payload;
                    _logger.info(accIdRegisterRsp.toJsonString(_logger));
                    break;
//                    TODO:: send role request
                case NS_ID_GEN_RSP:
                    _logger.info("Received NsID Generate Response");
                    NsIdGenerateRsp nsIdGenerateRsp = (NsIdGenerateRsp) payload;
                    _logger.info(nsIdGenerateRsp.toJsonString(_logger));
                    break;
                case NS_ID_REG_RSP:
                    _logger.info("Received NsID Register Response");
                    NsIdRegisterRsp nsIdRegisterRsp = (NsIdRegisterRsp) payload;
                    _logger.info(nsIdRegisterRsp.toJsonString(_logger));
                    break;
                case DIR_ID_GEN_REG_RSP:
                    _logger.info("Received DirID Register Response");
                    DirIdGenerateAndRegisterRsp dirIdGenerateAndRegisterRsp = (DirIdGenerateAndRegisterRsp) payload;
                    _logger.info(dirIdGenerateAndRegisterRsp.toJsonString(_logger));
                    break;
                case FILE_ID_GEN_REG_RSP:
                    _logger.info("Received FileID Register Response");
                    FileIdGenerateAndRegisterRsp fileIdGenerateAndRegisterRsp = (FileIdGenerateAndRegisterRsp) payload;
                    _logger.info(fileIdGenerateAndRegisterRsp.toJsonString(_logger));
                    break;
                case FILE_VERSION_ID_GEN_REG_RSP:
                    _logger.info("Received FileVersion Response");
                    FileVersionIdGenerateAndRegisterRsp fileVersionIdGenerateAndRegisterRsp = (FileVersionIdGenerateAndRegisterRsp) payload;
                    _logger.info(fileVersionIdGenerateAndRegisterRsp.toJsonString(_logger));
                    break;
                case STORAGE_CLS_META_ERROR_RSP:
                    StorageClsMetaErrorRsp rspError = (StorageClsMetaErrorRsp) payload;
                    _logger.info("Received error: " + new String(rspError.errorCauseInUtf8, StandardCharsets.UTF_8));
                    break;
                default:
                    _logger.info("Received Generic Response");
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}