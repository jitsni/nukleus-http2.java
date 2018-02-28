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
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.WINDOW_UPDATE;

/*

    Flyweight for HTTP2 WINDOW_UPDATE frame's payload

    +=+=============================================================+
    |R|              Window Size Increment (31)                     |
    +-+-------------------------------------------------------------+

 */
public class Http2WindowUpdatePayloadFW extends Flyweight
{

    private final AtomicBuffer payloadRO = new UnsafeBuffer(new byte[0]);

    public int size()
    {
        return buffer().getInt(offset(), BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    @Override
    public int limit() {
        return offset() + 4;
    }

    @Override
    public Http2WindowUpdatePayloadFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int payloadLength = sizeof();
        if (payloadLength != 4)
        {
            throw new IllegalArgumentException(String.format("Invalid WINDOW_UPDATE frame length=%d (must be 4)", payloadLength));
        }

        payloadRO.wrap(buffer, offset(), 4);

        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("WINDOW_UPDATE payload <length=%d>", sizeof());
    }

}

