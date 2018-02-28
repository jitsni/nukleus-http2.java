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
package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.ACK;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId.ENABLE_PUSH;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId.HEADER_TABLE_SIZE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId.INITIAL_WINDOW_SIZE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId.MAX_CONCURRENT_STREAMS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId.MAX_FRAME_SIZE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId.MAX_HEADER_LIST_SIZE;

/*
    Flyweight for HTTP2 SETTINGS frame's payload

    +=+=============================+===============================+
    |       Identifier (16)         |
    +-------------------------------+-------------------------------+
    |                        Value (32)                             |
    +---------------------------------------------------------------+
    |       ...                     |
    +-------------------------------+-------------------------------+
    |                        ...                                    |
    +---------------------------------------------------------------+

 */
public class Http2SettingsPayloadFW extends Http2FrameFW
{

    private final UnboundedListFW<Http2SettingFW> listFW = new UnboundedListFW<>(new Http2SettingFW());

    @Override
    public Http2FrameType type()
    {
        return SETTINGS;
    }

    public boolean ack()
    {
        return Http2Flags.ack(flags());
    }

    @Override
    public int streamId()
    {
        return 0;
    }

    public void accept(BiConsumer<Http2SettingsId, Long> consumer)
    {
        listFW.forEach(s -> consumer.accept(Http2SettingsId.get(s.id()), s.value()));
    }

    public long headerTableSize()
    {
        return settings(HEADER_TABLE_SIZE.id());
    }

    public long enablePush()
    {
        return settings(ENABLE_PUSH.id());
    }

    public long maxConcurrentStreams()
    {
        return settings(MAX_CONCURRENT_STREAMS.id());
    }

    public long initialWindowSize()
    {
        return settings(INITIAL_WINDOW_SIZE.id());
    }

    public long maxFrameSize()
    {
        return settings(MAX_FRAME_SIZE.id());
    }

    public long maxHeaderListSize()
    {
        return settings(MAX_HEADER_LIST_SIZE.id());
    }

    public long settings(int key)
    {
        long[] value = new long[] { -1L };

        // TODO https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        listFW.forEach(x ->
        {
            if (x.id() == key)
            {
                value[0] = x.value();
            }
        });
        return value[0];
    }

    @Override
    public Http2SettingsPayloadFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int streamId = super.streamId();
        if (streamId != 0)
        {
            throw new IllegalArgumentException(String.format("Invalid SETTINGS frame stream-id=%d", streamId));
        }

        Http2FrameType type = super.type();
        if (type != SETTINGS)
        {
            throw new IllegalArgumentException(String.format("Invalid SETTINGS frame type=%s", type));
        }

        int payloadLength = super.payloadLength();
        if (payloadLength%6 != 0)
        {
            throw new IllegalArgumentException(String.format("Invalid SETTINGS frame length=%d", payloadLength));
        }

        listFW.wrap(buffer, offset, limit());
        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("SETTINGS payload <length=%s>", sizeof());
    }

}

