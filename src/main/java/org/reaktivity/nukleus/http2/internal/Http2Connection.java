/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http2.internal;

import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.CLOSED;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.HALF_CLOSED_REMOTE;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.OPEN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.CONNECTION;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.KEEP_ALIVE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.PROXY_CONNECTION;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TRAILERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.UPGRADE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType.UNKNOWN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.String16FW;
import org.reaktivity.nukleus.http2.internal.types.StringFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AckFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;
import org.reaktivity.nukleus.route.RouteManager;

final class Http2Connection
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();
    private static final int FIN = 0x01;
    private static final int RST = 0x02;

    ServerStreamFactory factory;

    long sourceId;
    long authorization;
    int lastStreamId;
    long sourceRef;

    final ListFW.Builder<RegionFW.Builder, RegionFW> regionsRW;

    final WriteScheduler writeScheduler;

    final long networkReplyId;
    private final HpackContext decodeContext;
    private final HpackContext encodeContext;
    private final MessageFunction<RouteFW> wrapRoute;
    private final Http2Decoder decoder;

    final Int2ObjectHashMap<Http2Stream> http2Streams;      // HTTP2 stream-id --> Http2Stream
    final Long2ObjectHashMap<Http2Stream> regionStreams;      // region stream-id --> Http2Stream

    private int clientStreamCount;
    private int promisedStreamCount;
    private int maxClientStreamId;
    private int maxPushPromiseStreamId;

    private boolean goaway;
    Settings initialSettings;
    Settings localSettings;
    Settings remoteSettings;
    private boolean expectContinuation;
    private int expectContinuationStreamId;
    private boolean expectDynamicTableSizeUpdate = true;
    long http2OutWindow;
    long http2InWindow;

    private final Consumer<HpackHeaderFieldFW> headerFieldConsumer;
    private final HeadersContext headersContext = new HeadersContext();
    private final EncodeHeadersContext encodeHeadersContext = new EncodeHeadersContext();
    final Http2Writer http2Writer;
    MessageConsumer networkReply;
    RouteManager router;
    String sourceName;
    MutableDirectBuffer headersBuffer;
    int headersSlotPosition;
    MessageConsumer networkThrottle;
    long networkId;
    MutableDirectBuffer regionsBuf;
    boolean pendingNetworkAckFin;
    boolean pendingNetworkAckRst;
    boolean networkReplyTransferRst;
    boolean networkReplyTransferFin;

    final RegionsManager regionsManager;

    Http2Connection(
        ServerStreamFactory factory,
        RouteManager router,
        MessageConsumer networkThrottle,
        long networkId,
        long networkReplyId,
        MessageConsumer networkReply,
        MessageFunction<RouteFW> wrapRoute)
    {
        this.factory = factory;
        this.router = router;
        this.wrapRoute = wrapRoute;
        this.networkReplyId = networkReplyId;
        http2Streams = new Int2ObjectHashMap<>();
        regionStreams = new Long2ObjectHashMap<>();
        localSettings = new Settings();
        remoteSettings = new Settings();
        decodeContext = new HpackContext(localSettings.headerTableSize, false);
        encodeContext = new HpackContext(remoteSettings.headerTableSize, true);
        http2Writer = factory.http2Writer;
        writeScheduler = new Http2WriteScheduler(factory.memoryManager, this, networkReply, http2Writer, this.networkReplyId);
        http2InWindow = localSettings.initialWindowSize;
        http2OutWindow = remoteSettings.initialWindowSize;
        this.networkReply = networkReply;
        this.networkThrottle = networkThrottle;
        this.networkId = networkId;
        this.regionsManager = new RegionsManager();

        BiConsumer<DirectBuffer, DirectBuffer> nameValue =
                ((BiConsumer<DirectBuffer, DirectBuffer>)this::collectHeaders)
                        .andThen(this::mapToHttp)
                        .andThen(this::validatePseudoHeaders)
                        .andThen(this::uppercaseHeaders)
                        .andThen(this::connectionHeaders)
                        .andThen(this::contentLengthHeader)
                        .andThen(this::teHeader);

        Consumer<HpackHeaderFieldFW> consumer = this::validateHeaderFieldType;
        consumer = consumer.andThen(this::dynamicTableSizeUpdate);
        this.headerFieldConsumer = consumer.andThen(h -> decodeHeaderField(h, nameValue));
        regionsRW = new ListFW.Builder<>(new RegionFW.Builder(), new RegionFW());
        regionsBuf = new UnsafeBuffer(new byte[4096]);          // TODO use memoryManager
        regionsRW.wrap(regionsBuf, 0, regionsBuf.capacity());
        this.decoder = new Http2Decoder(factory.memoryManager, factory.supplyBufferBuilder, localSettings.maxFrameSize,
                regionsRW,
                factory.prefaceRO, factory.frameHeaderRO, factory.http2RO,
                this::processPreface, this::processFrame);
    }

    void processPreface(
        Http2PrefaceFW preface)
    {
        ackAll();

        regionsRW.wrap(regionsBuf, 0, regionsBuf.capacity());
    }

    void processFrame(
        Http2FrameFW frame)
    {
        processHttp2Frame(frame);

        regionsRW.wrap(regionsBuf, 0, regionsBuf.capacity());
    }

    void ackAll()
    {
        factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                     .streamId(networkId);
        Http2Decoder.ackAll(regionsRW.build(), factory.ackRW);
        AckFW ack = factory.ackRW.build();
        System.out.println("ackAll");
        factory.doReqAck(regionsManager, networkThrottle, ack);
    }

    void ackForData()
    {
        factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                     .streamId(networkId);
        Http2Decoder.ackForData(regionsRW.build(), factory.http2RO, factory.http2DataRO, factory.ackRW);
        AckFW ack = factory.ackRW.build();
        System.out.println("ackForData");
        factory.doReqAck(regionsManager, networkThrottle, ack);
    }

    void processUnexpected(
            long streamId)
    {
        factory.doReset(networkReply, streamId);
        cleanConnection();
    }

    void cleanConnection()
    {
        releaseHeadersSlot();

        writeScheduler.close();
    }

    void handleBegin(BeginFW beginRO)
    {
        this.sourceId = beginRO.streamId();
        this.authorization = beginRO.authorization();
        this.sourceRef = beginRO.sourceRef();
        this.sourceName = beginRO.source().asString();
        initialSettings = new Settings(factory.config.serverConcurrentStreams());
        writeScheduler.settings(initialSettings.maxConcurrentStreams);
    }

    void handleData(TransferFW dataRO)
    {
        dataRO.regions().forEach(r -> regionsManager.add(r.address(), r.length()));
        //regionsManager.print();
        decoder.decode(dataRO.regions());
    }

    void onNetworkTransferFin(TransferFW end)
    {
        // FINs reply stream
        // TODO wait for HTTP2 frames to be written ?
        if (!networkReplyTransferFin)
        {
            networkReplyTransferFin = true;
            TransferFW transfer = factory.transferRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                     .streamId(networkReplyId)
                                     .flags(FIN)
                                     .build();
            factory.doTransfer(networkReply, transfer);
        }

        http2Streams.forEach((i, s) -> s.onNetworkTransferFin());
        pendingNetworkAckFin = true;
        doNetworkAckFin();

        writeScheduler.doEnd();
    }

    void doNetworkAckFin()
    {
        if (pendingNetworkAckFin && http2Streams.isEmpty())
        {
            pendingNetworkAckFin = false;

            AckFW ack = factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                     .streamId(networkId)
                                     .flags(FIN)
                                     .build();
            factory.doAck(networkThrottle, ack);

            cleanConnection();
        }
    }

    void onNetworkTransferRst()
    {
        // Aborts reply stream
        if (!networkReplyTransferRst)
        {
            networkReplyTransferRst = true;
            factory.doAbort(networkReply, networkReplyId);
        }

        // Wait until all application ACKs
        pendingNetworkAckRst = true;
        http2Streams.forEach((i, s) -> s.onNetworkTransferRst());
        doNetworkAckRst();
    }

    void doNetworkAckRst()
    {
        if (pendingNetworkAckRst && http2Streams.isEmpty())
        {
            pendingNetworkAckRst = false;

            AckFW ack = factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                     .streamId(networkId)
                                     .flags(RST)
                                     .build();
            factory.doAck(networkThrottle, ack);

            cleanConnection();
        }
    }

    void onNetworkReplyAckFin()
    {
        http2Streams.forEach((i, s) -> s.onNetworkReplyAckFin());
        doNetworkAckFin();
    }

    void onNetworkReplyAckRst()
    {
        http2Streams.forEach((i, s) -> s.onNetworkReplyAckRst());
        pendingNetworkAckRst = true;
        doNetworkAckRst();
    }

    private boolean acquireHeadersSlot()
    {
        if (headersBuffer == null)
        {
            headersBuffer = new UnsafeBuffer(new byte[8192]);       // TODO use memoryManager
        }
        return true;
    }

    private void releaseHeadersSlot()
    {
        headersBuffer = null;
        headersSlotPosition = 0;
    }

    /*
     * Assembles a complete HTTP2 headers (including any continuations) if any.
     *
     * @return true if a complete HTTP2 headers is assembled or any other frame
     *         false otherwise
     */
    private boolean http2HeadersAvailable()
    {
        if (expectContinuation)
        {
            if (factory.http2RO.type() != Http2FrameType.CONTINUATION || factory.http2RO.streamId() != expectContinuationStreamId)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return false;
            }
        }
        else if (factory.http2RO.type() == Http2FrameType.CONTINUATION)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return false;
        }
        switch (factory.http2RO.type())
        {
            case HEADERS:
                int streamId = factory.http2RO.streamId();
                if (streamId == 0 || streamId % 2 != 1 || streamId <= maxClientStreamId)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }

                factory.headersRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                int parentStreamId = factory.headersRO.parentStream();
                if (parentStreamId == streamId)
                {
                    // 5.3.1 A stream cannot depend on itself
                    streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }
                if (factory.headersRO.dataLength() < 0)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }

                return http2HeadersAvailable(factory.headersRO.buffer(), factory.headersRO.dataOffset(),
                        factory.headersRO.dataLength(), factory.headersRO.endHeaders());

            case CONTINUATION:
                factory.continationRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                DirectBuffer payload = factory.continationRO.payload();
                boolean endHeaders = factory.continationRO.endHeaders();

                return http2HeadersAvailable(payload, 0, payload.capacity(), endHeaders);
        }

        return true;
    }

    /*
     * Assembles a complete HTTP2 headers (including any continuations) and the
     * flyweight is wrapped with the buffer (it could be given buffer or slab)
     *
     * @return true if a complete HTTP2 headers is assembled
     *         false otherwise
     */
    private boolean http2HeadersAvailable(DirectBuffer buffer, int offset, int length, boolean endHeaders)
    {
        if (endHeaders)
        {
            if (headersSlotPosition > 0)
            {
                headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
                headersSlotPosition += length;
                buffer = headersBuffer;
                offset = 0;
                length = headersSlotPosition;
            }
            int maxLimit = offset + length;
            expectContinuation = false;
            releaseHeadersSlot();           // early release, but fine
            factory.blockRO.wrap(buffer, offset, maxLimit);
            return true;
        }
        else
        {
            if (!acquireHeadersSlot())
            {
                return false;
            }
            headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
            headersSlotPosition += length;
            expectContinuation = true;
            expectContinuationStreamId = factory.headersRO.streamId();
        }

        return false;
    }

    private void processHttp2Frame(Http2FrameFW http2RO)
    {
        Http2FrameType http2FrameType = factory.http2RO.type();
        System.out.printf("-> recv %s\n", factory.http2RO);
        // Assembles HTTP2 HEADERS and its CONTINUATIONS frames, if any
        if (!http2HeadersAvailable())
        {
            return;
        }
        switch (http2FrameType)
        {
            case DATA:
                doData();
                break;
            case HEADERS:   // fall-through
            case CONTINUATION:
                doHeaders();
                break;
            case PRIORITY:
                doPriority();
                break;
            case RST_STREAM:
                doRst();
                break;
            case SETTINGS:
                doSettings();
                break;
            case PUSH_PROMISE:
                doPushPromise();
                break;
            case PING:
                doPing();
                break;
            case GO_AWAY:
                doGoAway();
                break;
            case WINDOW_UPDATE:
                doWindow();
                break;
            default:
                // Ignore and discard unknown frame
        }

    }

    private void doGoAway()
    {
        ackAll();

        int streamId = factory.http2RO.streamId();
        if (goaway)
        {
            if (streamId != 0)
            {
                processUnexpected(sourceId);
            }
        }
        else
        {
            goaway = true;
            Http2ErrorCode errorCode = (streamId != 0) ? Http2ErrorCode.PROTOCOL_ERROR : Http2ErrorCode.NO_ERROR;
            remoteSettings.enablePush = false;      // no new streams
            error(errorCode);
        }
    }

    private void doPushPromise()
    {
        ackAll();

        error(Http2ErrorCode.PROTOCOL_ERROR);
    }

    private void doPriority()
    {
        ackAll();

        int streamId = factory.http2RO.streamId();
        if (streamId == 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        int payloadLength = factory.http2RO.payloadLength();
        if (payloadLength != 5)
        {
            streamError(streamId, Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.priorityRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
        int parentStreamId = factory.priorityRO.parentStream();
        if (parentStreamId == streamId)
        {
            // 5.3.1 A stream cannot depend on itself
            streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
    }

    private void doHeaders()
    {
        ackAll();

        int streamId = factory.http2RO.streamId();

        Http2Stream stream = http2Streams.get(streamId);
        if (stream != null)
        {
            // TODO trailers
        }
        if (streamId <= maxClientStreamId)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        maxClientStreamId = streamId;

        if (clientStreamCount + 1 > localSettings.maxConcurrentStreams)
        {
            streamError(streamId, stream, Http2ErrorCode.REFUSED_STREAM);
            return;
        }

        State state = factory.http2RO.endStream() ? HALF_CLOSED_REMOTE : OPEN;

        headersContext.reset();

        factory.httpBeginExRW.wrap(factory.scratch, 0, factory.scratch.capacity());

        factory.blockRO.forEach(headerFieldConsumer);
        // All HTTP/2 requests MUST include exactly one valid value for the
        // ":method", ":scheme", and ":path" pseudo-header fields, unless it is
        // a CONNECT request (Section 8.3).  An HTTP request that omits
        // mandatory pseudo-header fields is malformed
        if (!headersContext.error() && (headersContext.method != 1 || headersContext.scheme != 1 || headersContext.path != 1))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
        if (headersContext.error())
        {
            if (headersContext.streamError != null)
            {
                streamError(streamId, stream, headersContext.streamError);
                return;
            }
            if (headersContext.connectionError != null)
            {
                error(headersContext.connectionError);
                return;
            }
        }

        RouteFW route = resolveTarget(sourceRef, sourceName, headersContext.headers);
        if (route == null)
        {
            noRoute(streamId);
        }
        else
        {
            followRoute(streamId, state, route);
        }
    }

    private void followRoute(int streamId, State state, RouteFW route)
    {
        final String applicationName = route.target().asString();
        final MessageConsumer applicationTarget = router.supplyTarget(applicationName);
        HttpWriter httpWriter = factory.httpWriter;
        Http2Stream stream = newStream(streamId, state, applicationTarget, httpWriter);
        final long targetRef = route.targetRef();

        stream.contentLength = headersContext.contentLength;

        HttpBeginExFW beginEx = factory.httpBeginExRW.build();
        httpWriter.doHttpBegin(applicationTarget, stream.targetId, targetRef, stream.correlationId,
                beginEx.buffer(), beginEx.offset(), beginEx.sizeof());
        router.setThrottle(applicationName, stream.targetId, stream::onThrottle);

        if (factory.headersRO.endStream())
        {
            factory.doEnd(applicationTarget, stream.targetId);
        }
    }

    // No route for the HTTP2 request, send 404 on the corresponding HTTP2 stream
    private void noRoute(int streamId)
    {
        ListFW<HttpHeaderFW> headers =
                factory.headersRW.wrap(factory.errorBuf, 0, factory.errorBuf.capacity())
                                 .item(b -> b.name(":status").value("404"))
                                 .build();

        writeScheduler.headers(streamId, Http2Flags.END_STREAM, headers);
    }

    private void doRst()
    {
        ackAll();

        int streamId = factory.http2RO.streamId();
        if (streamId == 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        int payloadLength = factory.http2RO.payloadLength();
        if (payloadLength != 4)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        Http2Stream stream = http2Streams.get(streamId);
        if (stream == null || stream.state == State.IDLE)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
        }
        else
        {
            stream.onRstStream();
        }
    }

    void closeStream(Http2Stream stream)
    {
        if (stream.state != CLOSED)
        {
            stream.state = CLOSED;

            if (stream.isClientInitiated())
            {
                clientStreamCount--;
            }
            else
            {
                promisedStreamCount--;
            }
            factory.correlations.remove(stream.targetId);
            http2Streams.remove(stream.http2StreamId);
        }
    }

    private void doWindow()
    {
        ackAll();

        int streamId = factory.http2RO.streamId();
        if (factory.http2RO.payloadLength() != 4)
        {
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            else
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }
        if (streamId != 0)
        {
            State state = state(streamId);
            if (state == State.IDLE)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            Http2Stream stream = http2Streams.get(streamId);
            if (stream == null)
            {
                // A receiver could receive a WINDOW_UPDATE frame on a "half-closed (remote)" or "closed" stream.
                // A receiver MUST NOT treat this as an error
                return;
            }
        }
        factory.http2WindowRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        // 6.9 WINDOW_UPDATE - legal range for flow-control window increment is 1 to 2^31-1 octets.
        if (factory.http2WindowRO.size() < 1)
        {
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            else
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }

        // 6.9.1 A sender MUST NOT allow a flow-control window to exceed 2^31-1 octets.
        if (streamId == 0)
        {
            http2OutWindow += factory.http2WindowRO.size();
            System.out.printf("c.http2OutWindow=%d\n", http2OutWindow);

            if (http2OutWindow > Integer.MAX_VALUE)
            {
                error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                return;
            }
            writeScheduler.onHttp2Window();
        }
        else
        {
            Http2Stream stream = http2Streams.get(streamId);
            stream.http2OutWindow += factory.http2WindowRO.size();
            System.out.printf("s.http2OutWindow=%d\n", stream.http2OutWindow);
            if (stream.http2OutWindow > Integer.MAX_VALUE)
            {
                streamError(streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
                return;
            }
            writeScheduler.onHttp2Window(streamId);
        }

    }

    private void doData()
    {
        ackForData();

        int streamId = factory.http2RO.streamId();
        Http2Stream stream = http2Streams.get(streamId);

        if (streamId == 0 || stream == null || stream.state == State.IDLE)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (stream.state == HALF_CLOSED_REMOTE)
        {
            error(Http2ErrorCode.STREAM_CLOSED);
            return;
        }
        Http2DataFW dataRO = factory.http2DataRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(),
                factory.http2RO.limit());
        if (dataRO.dataLength() < 0)        // because of invalid padding length
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }

        //
        if (stream.http2InWindow < factory.http2RO.payloadLength() || http2InWindow < factory.http2RO.payloadLength())
        {
            streamError(streamId, stream, Http2ErrorCode.FLOW_CONTROL_ERROR);
            return;
        }
        http2InWindow -= factory.http2RO.payloadLength();
        stream.http2InWindow -= factory.http2RO.payloadLength();

        stream.totalData += factory.http2RO.payloadLength();

        if (dataRO.endStream())
        {
            // 8.1.2.6 A request is malformed if the value of a content-length header field does
            // not equal the sum of the DATA frame payload lengths
            if (stream.contentLength != -1 && stream.totalData != stream.contentLength)
            {
                streamError(streamId, stream, Http2ErrorCode.PROTOCOL_ERROR);
                //stream.httpWriteScheduler.doEnd(stream.targetId);
                return;
            }
            stream.state = State.HALF_CLOSED_REMOTE;
        }

        stream.onRequestData();
    }

    private void doSettings()
    {
        ackAll();

        if (factory.http2RO.streamId() != 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength()%6 != 0)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }

        factory.settingsRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (factory.settingsRO.ack() && factory.http2RO.payloadLength() != 0)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        if (!factory.settingsRO.ack())
        {
            factory.settingsRO.accept(this::doSetting);
            writeScheduler.settingsAck();
        }
        else
        {
            int update =  initialSettings.initialWindowSize - localSettings.initialWindowSize;
            for(Http2Stream http2Stream: http2Streams.values())
            {
                http2Stream.http2InWindow += update;           // http2InWindow can become negative
            }

            // now that peer acked our initial settings, can use them as our local settings
            localSettings = initialSettings;
        }
    }

    private void doSetting(Http2SettingsId id, Long value)
    {
        switch (id)
        {
            case HEADER_TABLE_SIZE:
                remoteSettings.headerTableSize = value.intValue();
                break;
            case ENABLE_PUSH:
                if (!(value == 0L || value == 1L))
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return;
                }
                remoteSettings.enablePush = (value == 1L);
                break;
            case MAX_CONCURRENT_STREAMS:
                remoteSettings.maxConcurrentStreams = value.intValue();
                break;
            case INITIAL_WINDOW_SIZE:
                if (value > Integer.MAX_VALUE)
                {
                    error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                    return;
                }
                int old = remoteSettings.initialWindowSize;
                remoteSettings.initialWindowSize = value.intValue();
                int update = value.intValue() - old;

                // 6.9.2. Initial Flow-Control Window Size
                // SETTINGS frame can alter the initial flow-control
                // window size for streams with active flow-control windows
                for(Http2Stream http2Stream: http2Streams.values())
                {
                    http2Stream.http2OutWindow += update;           // http2OutWindow can become negative
                    if (http2Stream.http2OutWindow > Integer.MAX_VALUE)
                    {
                        // 6.9.2. Initial Flow-Control Window Size
                        // An endpoint MUST treat a change to SETTINGS_INITIAL_WINDOW_SIZE that
                        // causes any flow-control window to exceed the maximum size as a
                        // connection error of type FLOW_CONTROL_ERROR.
                        error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                        return;
                    }
                }
                break;
            case MAX_FRAME_SIZE:
                if (value < Math.pow(2, 14) || value > Math.pow(2, 24) -1)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return;
                }
                remoteSettings.maxFrameSize = value.intValue();
                break;
            case MAX_HEADER_LIST_SIZE:
                remoteSettings.maxHeaderListSize = value.intValue();
                break;
            default:
                // Ignore the unkonwn setting
                break;
        }
    }

    private void doPing()
    {
        ackAll();

        if (factory.http2RO.streamId() != 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength() != 8)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.pingRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (!factory.pingRO.ack())
        {
            writeScheduler.pingAck(factory.pingRO.payload(), 0, factory.pingRO.payload().capacity());
        }
    }

    private State state(int streamId)
    {
        Http2Stream stream = http2Streams.get(streamId);
        if (stream != null)
        {
            return stream.state;
        }
        if (streamId%2 == 1)
        {
            if (streamId <= maxClientStreamId)
            {
                return State.CLOSED;
            }
        }
        else
        {
            if (streamId <= maxPushPromiseStreamId)
            {
                return State.CLOSED;
            }
        }
        return State.IDLE;
    }

    RouteFW resolveTarget(
            long sourceRef,
            String sourceName,
            Map<String, String> headers)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = factory.routeRO.wrap(b, o, l);
            OctetsFW extension = route.extension();
            if (sourceRef == route.sourceRef() && sourceName.equals(route.source().asString()))
            {
                Map<String, String> routeHeaders;
                if (extension.sizeof() == 0)
                {
                    routeHeaders = EMPTY_HEADERS;
                }
                else
                {
                    final HttpRouteExFW routeEx = extension.get(factory.httpRouteExRO::wrap);
                    routeHeaders = new LinkedHashMap<>();
                    routeEx.headers().forEach(h -> routeHeaders.put(h.name().asString(), h.value().asString()));
                }

                return headers.entrySet().containsAll(routeHeaders.entrySet());
            }
            return false;
        };

        return router.resolve(authorization, filter, wrapRoute);
    }

    void onNetworkReplyAck(AckFW ack)
    {
        writeScheduler.onAck(ack);
    }

    void onApplicationAck(AckFW ack)
    {
        factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                     .streamId(networkId);
        ack.regions().forEach(r -> factory.ackRW.regionsItem(
                m -> m.address(r.address()).length(r.length()).streamId(r.streamId())));
        AckFW newAck = factory.ackRW.build();

        System.out.println("onApplicationAck");
        factory.doReqAck(regionsManager, networkThrottle, newAck);
    }

    void error(Http2ErrorCode errorCode)
    {
        writeScheduler.goaway(lastStreamId, errorCode);
        writeScheduler.doEnd();
    }

    void streamError(int streamId, Http2ErrorCode errorCode)
    {
        Http2Stream stream = http2Streams.get(streamId);
        streamError(streamId, stream, errorCode);
    }

    void streamError(int streamId, Http2Stream stream, Http2ErrorCode errorCode)
    {
        if (stream != null)
        {
            doRstByUs(stream, errorCode);
        }
        else
        {
            writeScheduler.rst(streamId, errorCode);
        }
    }

    private int nextPromisedId()
    {
        maxPushPromiseStreamId += 2;
        return maxPushPromiseStreamId;
    }

    /*
     * @param streamId corresponding http2 stream-id on which service response
     *                 will be sent
     * @return a stream id on which PUSH_PROMISE can be sent
     *         -1 otherwise
     */
    private int findPushId(int streamId)
    {
        if (remoteSettings.enablePush && promisedStreamCount +1 < remoteSettings.maxConcurrentStreams)
        {
            // PUSH_PROMISE frames MUST only be sent on a peer-initiated stream
            if (streamId%2 == 0)
            {
                // Find a stream on which PUSH_PROMISE can be sent
                return http2Streams.entrySet()
                                   .stream()
                                   .map(Map.Entry::getValue)
                                   .filter(s -> (s.http2StreamId & 0x01) == 1)     // client-initiated stream
                                   .filter(s -> s.state == OPEN || s.state == HALF_CLOSED_REMOTE)
                                   .mapToInt(s -> s.http2StreamId)
                                   .findAny()
                                   .orElse(-1);
            }
            else
            {
                return streamId;        // client-initiated stream
            }
        }
        return -1;
    }

    private void doPromisedRequest(int http2StreamId, ListFW<HttpHeaderFW> headers)
    {
        Map<String, String> headersMap = new HashMap<>();
        headers.forEach(
                httpHeader -> headersMap.put(httpHeader.name().asString(), httpHeader.value().asString()));
        RouteFW route = resolveTarget(sourceRef, sourceName, headersMap);
        final String applicationName = route.target().asString();
        final MessageConsumer applicationTarget = router.supplyTarget(applicationName);
        HttpWriter httpWriter = factory.httpWriter;
        Http2Stream http2Stream = newStream(http2StreamId, HALF_CLOSED_REMOTE, applicationTarget, httpWriter);
        long targetId = http2Stream.targetId;
        long targetRef = route.targetRef();

        httpWriter.doHttpBegin(applicationTarget, targetId, targetRef, http2Stream.correlationId,
                hs -> headers.forEach(h -> hs.item(b -> b.name(h.name())
                                                         .value(h.value()))));
        router.setThrottle(applicationName, targetId, http2Stream::onThrottle);
        factory.doEnd(applicationTarget, targetId);
    }

    private Http2Stream newStream(int http2StreamId, State state, MessageConsumer applicationTarget, HttpWriter httpWriter)
    {
        assert http2StreamId != 0;

        Http2Stream http2Stream = new Http2Stream(factory, this, http2StreamId, state, applicationTarget, httpWriter);
        http2Streams.put(http2StreamId, http2Stream);

        Correlation correlation = new Correlation(http2Stream.correlationId, networkReplyId, writeScheduler,
                this::doPromisedRequest, this, http2StreamId, encodeContext, this::nextPromisedId, this::findPushId);

        factory.correlations.put(http2Stream.correlationId, correlation);
        if (http2Stream.isClientInitiated())
        {
            clientStreamCount++;
        }
        else
        {
            promisedStreamCount++;
        }
        return http2Stream;
    }

    private void validateHeaderFieldType(HpackHeaderFieldFW hf)
    {
        if (!headersContext.error() && hf.type() == UNKNOWN)
        {
            headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
        }
    }

    private void dynamicTableSizeUpdate(HpackHeaderFieldFW hf)
    {
        if (!headersContext.error())
        {
            switch (hf.type())
            {
                case INDEXED:
                case LITERAL:
                    expectDynamicTableSizeUpdate = false;
                    break;
                case UPDATE:
                    if (!expectDynamicTableSizeUpdate)
                    {
                        // dynamic table size update MUST occur at the beginning of the first header block
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    int maxTableSize = hf.tableSize();
                    if (maxTableSize > localSettings.headerTableSize)
                    {
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    decodeContext.updateSize(hf.tableSize());
                    break;
            }
        }
    }

    private void validatePseudoHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            if (name.capacity() > 0 && name.getByte(0) == ':')
            {
                // All pseudo-header fields MUST appear in the header block before regular header fields
                if (headersContext.regularHeader)
                {
                    headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    return;
                }
                // request pseudo-header fields MUST be one of :authority, :method, :path, :scheme,
                int index = decodeContext.index(name);
                switch (index)
                {
                    case 1:             // :authority
                        break;
                    case 2:             // :method
                        headersContext.method++;
                        break;
                    case 4:             // :path
                        if (value.capacity() > 0)       // :path MUST not be empty
                        {
                            headersContext.path++;
                        }
                        break;
                    case 6:             // :scheme
                        headersContext.scheme++;
                        break;
                    default:
                        headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                        return;
                }
            }
            else
            {
                headersContext.regularHeader = true;
            }
        }
    }

    private void connectionHeaders(DirectBuffer name, DirectBuffer value)
    {

        if (!headersContext.error() && name.equals(HpackContext.CONNECTION))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
    }

    private void contentLengthHeader(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error() && name.equals(decodeContext.nameBuffer(28)))
        {
            String contentLength = value.getStringWithoutLengthUtf8(0, value.capacity());
            headersContext.contentLength = Long.parseLong(contentLength);
        }
    }

    // 8.1.2.2 TE header MUST NOT contain any value other than "trailers".
    private void teHeader(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error() && name.equals(TE) && !value.equals(TRAILERS))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
    }

    private void uppercaseHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            for(int i=0; i < name.capacity(); i++)
            {
                if (name.getByte(i) >= 'A' && name.getByte(i) <= 'Z')
                {
                    headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                }
            }
        }
    }

    // Collect headers into map to resolve target
    // TODO avoid this
    private void collectHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            String nameStr = name.getStringWithoutLengthUtf8(0, name.capacity());
            String valueStr = value.getStringWithoutLengthUtf8(0, value.capacity());
            headersContext.headers.put(nameStr, valueStr);
        }
    }

    // Writes HPACK header field to http representation in a buffer
    private void mapToHttp(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            factory.httpBeginExRW.headersItem(item -> item.name(name, 0, name.capacity())
                                                          .value(value, 0, value.capacity()));
        }
    }

    private void decodeHeaderField(HpackHeaderFieldFW hf,
                                   BiConsumer<DirectBuffer, DirectBuffer> nameValue)
    {
        int index;
        DirectBuffer name = null;
        DirectBuffer value = null;

        switch (hf.type())
        {
            case INDEXED :
                index = hf.index();
                if (!decodeContext.valid(index))
                {
                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                name = decodeContext.nameBuffer(index);
                value = decodeContext.valueBuffer(index);
                nameValue.accept(name, value);
                break;

            case LITERAL :
                HpackLiteralHeaderFieldFW literalRO = hf.literal();
                if (literalRO.error())
                {
                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                switch (literalRO.nameType())
                {
                    case INDEXED:
                    {
                        index = literalRO.nameIndex();
                        name = decodeContext.nameBuffer(index);

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        value = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(value, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            value = new UnsafeBuffer(dst, 0, length);
                        }
                        nameValue.accept(name, value);
                    }
                    break;
                    case NEW:
                    {
                        HpackStringFW nameRO = literalRO.nameLiteral();
                        name = nameRO.payload();
                        if (nameRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(name, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            name = new UnsafeBuffer(dst, 0, length);
                        }

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        value = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(value, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            value = new UnsafeBuffer(dst, 0, length);
                        }
                        nameValue.accept(name, value);
                    }
                    break;
                }
                if (literalRO.literalType() == INCREMENTAL_INDEXING)
                {
                    // make a copy for name and value as they go into dynamic table (outlives current frame)
                    MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[name.capacity()]);
                    nameCopy.putBytes(0, name, 0, name.capacity());
                    MutableDirectBuffer valueCopy = new UnsafeBuffer(new byte[value.capacity()]);
                    valueCopy.putBytes(0, value, 0, value.capacity());
                    decodeContext.add(nameCopy, valueCopy);
                }
                break;
        }
    }


    void mapPushPromise(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        httpHeaders.forEach(h -> builder.header(b -> mapHeader(h, b)));
    }

    void mapHeaders(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        encodeHeadersContext.reset();

        httpHeaders.forEach(this::status)                       // checks if there is :status
                   .forEach(this::accessControlAllowOrigin)     // checks if there is access-control-allow-origin
                   .forEach(this::connectionHeaders);           // collects all connection headers
        if (!encodeHeadersContext.status)
        {
            builder.header(b -> b.indexed(8));          // no mandatory :status header, add :status: 200
        }

        httpHeaders.forEach(h ->
        {
            if (validHeader(h))
            {
                builder.header(b -> mapHeader(h, b));
            }
        });

        if (factory.config.accessControlAllowOrigin() && !encodeHeadersContext.accessControlAllowOrigin)
        {
            builder.header(b -> b.literal(l -> l.type(WITHOUT_INDEXING).name(20).value(DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN)));
        }
    }

    private void status(HttpHeaderFW httpHeader)
    {
        if (!encodeHeadersContext.status)
        {
            StringFW name = httpHeader.name();
            String16FW value = httpHeader.value();
            factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
            factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

            if (factory.nameRO.equals(encodeContext.nameBuffer(8)))
            {
                encodeHeadersContext.status = true;
            }
        }
    }

    // Checks if response has access-control-allow-origin header
    private void accessControlAllowOrigin(HttpHeaderFW httpHeader)
    {
        if (factory.config.accessControlAllowOrigin() && !encodeHeadersContext.accessControlAllowOrigin)
        {
            StringFW name = httpHeader.name();
            String16FW value = httpHeader.value();
            factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
            factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

            if (factory.nameRO.equals(encodeContext.nameBuffer(20)))
            {
                encodeHeadersContext.accessControlAllowOrigin = true;
            }
        }
    }

    private void connectionHeaders(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer

        if (factory.nameRO.equals(CONNECTION))
        {
            String[] headers = value.asString().split(",");
            for (String header : headers)
            {
                encodeHeadersContext.connectionHeaders.add(header.trim());
            }
        }
    }

    private boolean validHeader(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

        // Removing 8.1.2.1 Pseudo-Header Fields
        // Not sending error as it will allow requests to loop back
        if (factory.nameRO.equals(encodeContext.nameBuffer(1)) ||          // :authority
                factory.nameRO.equals(encodeContext.nameBuffer(2)) ||      // :method
                factory.nameRO.equals(encodeContext.nameBuffer(4)) ||      // :path
                factory.nameRO.equals(encodeContext.nameBuffer(6)))        // :scheme
        {
            return false;
        }

        // Removing 8.1.2.2 connection-specific header fields from response
        if (factory.nameRO.equals(encodeContext.nameBuffer(57)) ||         // transfer-encoding
                factory.nameRO.equals(CONNECTION) ||
                factory.nameRO.equals(KEEP_ALIVE) ||
                factory.nameRO.equals(PROXY_CONNECTION) ||
                factory.nameRO.equals(UPGRADE))
        {
            return false;
        }

        // Removing any header that is nominated by Connection header field
        for(String connectionHeader: encodeHeadersContext.connectionHeaders)
        {
            if (name.asString().equals(connectionHeader))
            {
                return false;
            }
        }

        return true;
    }

    // Map http1.1 header to http2 header field in HEADERS, PUSH_PROMISE request
    private void mapHeader(HttpHeaderFW httpHeader, HpackHeaderFieldFW.Builder builder)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

        int index = encodeContext.index(factory.nameRO, factory.valueRO);
        if (index != -1)
        {
            // Indexed
            builder.indexed(index);
        }
        else
        {
            // Literal
            builder.literal(literalBuilder -> buildLiteral(literalBuilder, encodeContext));
        }
    }

    // Building Literal representation of header field
    // TODO dynamic table, huffman, never indexed
    private void buildLiteral(
            HpackLiteralHeaderFieldFW.Builder builder,
            HpackContext hpackContext)
    {
        int nameIndex = hpackContext.index(factory.nameRO);
        builder.type(WITHOUT_INDEXING);
        if (nameIndex != -1)
        {
            builder.name(nameIndex);
        }
        else
        {
            builder.name(factory.nameRO, 0, factory.nameRO.capacity());
        }
        builder.value(factory.valueRO, 0, factory.valueRO.capacity());
    }


    void handleHttpBegin(BeginFW begin, MessageConsumer applicationReplyThrottle, long applicationReplyId,
                         Correlation correlation)
    {
        OctetsFW extension = begin.extension();
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);
        if (stream == null)
        {
            factory.doReset(applicationReplyThrottle, applicationReplyId);
        }
        else
        {
            stream.applicationReplyThrottle = applicationReplyThrottle;
            stream.applicationReplyId = applicationReplyId;

            if (extension.sizeof() > 0)
            {
                HttpBeginExFW beginEx = extension.get(factory.beginExRO::wrap);
                writeScheduler.headers(correlation.http2StreamId, Http2Flags.NONE, beginEx.headers());
            }
        }
    }

    void onApplicationReplyTransfer(TransferFW data, Correlation correlation)
    {
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);
        if (stream != null)
        {
            data.regions().forEach(r -> regionStreams.put(r.streamId(), stream));
            stream.onResponseData(data);
        }
        else
        {

        }
        //writeScheduler.data(correlation.http2StreamId, data);

//        OctetsFW extension = dataRO.extension();
//        OctetsFW payload = dataRO.payload();
//
//        if (extension.sizeof() > 0)
//        {
//
//            int pushStreamId = correlation.pushStreamIds.applyAsInt(correlation.http2StreamId);
//            if (pushStreamId != -1)
//            {
//                int promisedStreamId = correlation.promisedStreamIds.getAsInt();
//                Http2DataExFW dataEx = extension.get(factory.dataExRO::wrap);
//                writeScheduler.pushPromise(pushStreamId, promisedStreamId, dataEx.headers());
//                correlation.pushHandler.accept(promisedStreamId, dataEx.headers());
//            }
//        }
//        if (payload != null)
//        {
//            Http2Stream stream = http2Streams.get(correlation.http2StreamId);
//            if (stream != null)
//            {
//                stream.applicationReplyBudget -= dataRO.length() + dataRO.padding();
//                if (stream.applicationReplyBudget < 0)
//                {
//                    doRstByUs(stream, Http2ErrorCode.INTERNAL_ERROR);
//                    return;
//                }
//            }
//
//            writeScheduler.data(correlation.http2StreamId, payload.buffer(), payload.offset(), payload.sizeof());
//        }

    }

    void processTransportAck(int flags, long address, int length, long targetId)
    {
        Http2Stream stream = regionStreams.get(targetId);
        if (stream != null)
        {
            stream.onNetworkReplyAck(flags, address, length, targetId);
        }
    }

    void onApplicationReplyTransferFin(TransferFW data, Correlation correlation)
    {
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);

        if (stream != null)
        {
            stream.onApplicationReplyFin();
        }
    }

    void onApplicationAckRst()
    {
        doNetworkAckRst();
    }

    void onApplicationAckFin()
    {
        doNetworkAckFin();
    }

    void onApplicationReplyTransferRst(TransferFW abort, Correlation correlation)
    {
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);

        if (stream != null)
        {
            stream.onApplicationReplyTransferRst();

        }
    }

    void doRstByUs(Http2Stream stream, Http2ErrorCode errorCode)
    {
        stream.doRstStream(errorCode);
    }

    enum State
    {
        IDLE,
        RESERVED_LOCAL,
        RESERVED_REMOTE,
        OPEN,
        HALF_CLOSED_LOCAL,
        HALF_CLOSED_REMOTE,
        CLOSED
    }

    private static final class HeadersContext
    {
        Http2ErrorCode connectionError;
        Map<String, String> headers = new HashMap<>();
        int method;
        int scheme;
        int path;
        boolean regularHeader;
        Http2ErrorCode streamError;
        long contentLength = -1;

        void reset()
        {
            connectionError = null;
            headers.clear();
            method = 0;
            scheme = 0;
            path = 0;
            regularHeader = false;
            streamError = null;
            contentLength = -1;
        }

        boolean error()
        {
            return streamError != null || connectionError != null;
        }
    }

    private static final class EncodeHeadersContext
    {
        boolean status;
        boolean accessControlAllowOrigin;
        final List<String> connectionHeaders = new ArrayList<>();

        void reset()
        {
            status = false;
            accessControlAllowOrigin = false;
            connectionHeaders.clear();
        }

    }



}
