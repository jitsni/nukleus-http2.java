/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.http2.internal.client;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.Http2Configuration;
import org.reaktivity.nukleus.http2.internal.Http2NukleusFactorySpi;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ContinuationFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2WindowUpdateFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

import java.util.LinkedHashMap;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.function.LongUnaryOperator;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ClientStreamFactory implements StreamFactory
{
    static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer(Http2NukleusFactorySpi.NAME.getBytes(UTF_8));

    // read only
    private final RouteFW routeRO = new RouteFW();
    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();
    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();
    final HttpRouteExFW httpRouteExRO = new HttpRouteExFW();
    final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    // http2 frames
    final Http2FrameFW http2RO = new Http2FrameFW();
    final Http2HeadersFW headersRO = new Http2HeadersFW();
    final Http2ContinuationFW continuationRO = new Http2ContinuationFW();
    final HpackHeaderBlockFW blockRO = new HpackHeaderBlockFW();
    final Http2SettingsFW settingsRO = new Http2SettingsFW();
    final Http2DataFW http2DataRO = new Http2DataFW();
    final Http2PingFW http2PingRO = new Http2PingFW();
    final Http2WindowUpdateFW http2WindowUpdateRO = new Http2WindowUpdateFW();

    final RouteManager router;
    final MutableDirectBuffer writeBuffer;
    final BufferPool bufferPool;
    final LongUnaryOperator supplyInitialId;
    final LongUnaryOperator supplyReplyId;
    final Long2ObjectHashMap<Http2ClientConnection> correlations;
    final Http2ClientConnectionManager http2ConnectionManager;

    // builders
    final BeginFW.Builder beginRW = new BeginFW.Builder();
    final AbortFW.Builder abortRW = new AbortFW.Builder();
    final WindowFW.Builder windowRW = new WindowFW.Builder();
    final ResetFW.Builder resetRW = new ResetFW.Builder();
    final EndFW.Builder endRW = new EndFW.Builder();
    final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    final DataFW.Builder dataRW = new DataFW.Builder();

    final DirectBuffer nameRO = new UnsafeBuffer(new byte[0]);
    final DirectBuffer valueRO = new UnsafeBuffer(new byte[0]);
    final UnsafeBuffer scratch = new UnsafeBuffer(new byte[8192]);  // TODO

    public ClientStreamFactory(
        Http2Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        Long2ObjectHashMap<Http2ClientConnection> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);

        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.correlations = requireNonNull(correlations);
        this.http2ConnectionManager = new Http2ClientConnectionManager(this);
}

    @Override
    public MessageConsumer newStream(int msgTypeId, DirectBuffer buffer, int index, int length, MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newAcceptStream(begin, throttle);
        }
        else
        {
            newStream = newConnectReplyStream(begin, throttle);
        }

        return newStream;
   }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer networkReply)
    {
        final long networkRouteId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) -> true;

        final RouteFW route = router.resolve(networkRouteId, begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ClientAcceptStream(this, networkReply, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(BeginFW begin, MessageConsumer connectReplyThrottle)
    {
        final long connectReplyId = begin.streamId();
        return new ClientConnectReplyStream(this, connectReplyThrottle, connectReplyId)::handleStream;
    }

    // methods for sending frames on a stream
    void doBegin(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .trace(traceId)
                                     .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doAbort(
        final MessageConsumer target,
        final long targetId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .extension(e -> e.reset())
                .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int writableBytes,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .credit(writableBytes)
                .padding(padding)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
            final MessageConsumer throttle,
            final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    void doEnd(
        final MessageConsumer target,
        final long targetId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doData(
        final MessageConsumer target,
        final long targetId,
        DirectBuffer buffer,
        int offset,
        int length)
    {
        final DataFW end = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(buffer, offset, length)
                .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    RouteFW resolveTarget(
        long networkRouteId,
        long authorization,
        Map<String, String> headers)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, o + l);
            OctetsFW extension = route.extension();
            Map<String, String> routeHeaders;
            if (extension.sizeof() == 0)
            {
                routeHeaders = Http2ClientConnection.EMPTY_HEADERS;
            }
            else
            {
                final HttpRouteExFW routeEx = extension.get(httpRouteExRO::wrap);
                routeHeaders = new LinkedHashMap<>();
                routeEx.headers().forEach(h -> routeHeaders.put(h.name().asString(), h.value().asString()));
            }

            return headers.entrySet().containsAll(routeHeaders.entrySet());
        };

        return router.resolve(networkRouteId, authorization, filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }
}
