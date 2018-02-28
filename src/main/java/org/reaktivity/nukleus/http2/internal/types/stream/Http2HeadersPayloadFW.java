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
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.END_HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.END_STREAM;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.PADDED;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.PRIORITY;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;

/*

    Flyweight for HTTP2 HEADERS frame's payload

    +=+=============+===============================================+
    |Pad Length? (8)|
    +-+-------------+-----------------------------------------------+
    |E|                 Stream Dependency? (31)                     |
    +-+-------------+-----------------------------------------------+
    |  Weight? (8)  |
    +-+-------------+-----------------------------------------------+
    |                   Header Block Fragment (*)                 ...
    +---------------------------------------------------------------+
    |                           Padding (*)                       ...
    +---------------------------------------------------------------+

 */
public class Http2HeadersPayloadFW extends Flyweight
{

    boolean padded;
    boolean priority;

    public int dataOffset()
    {
        int dataOffset = offset();
        if (padded)
        {
            dataOffset++;        // +1 for Pad Length
        }
        if (priority)
        {
            dataOffset += 5;      // +4 for Stream Dependency, +1 for Weight
        }
        return dataOffset;
    }

    public int parentStream()
    {
        if (priority)
        {
            int dependencyOffset = offset();
            if (padded)
            {
                dependencyOffset++;
            }
            return buffer().getInt(dependencyOffset, BIG_ENDIAN) & 0x7F_FF_FF_FF;
        }
        else
        {
            return 0;
        }
    }

    public int dataLength()
    {
        int dataLength = sizeof();
        if (padded)
        {
            int paddingLength = buffer().getByte(offset()) & 0xff;
            dataLength -= (paddingLength + 1);    // -1 for Pad Length, -Padding
        }

        if (priority)
        {
            dataLength -= 5;    // -4 for Stream Dependency, -1 for Weight
        }

        return dataLength;
    }

    @Override
    public int limit() {
        return maxLimit();
    }

    @Override
    public Http2HeadersPayloadFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        checkLimit(limit(), maxLimit);
        return this;
    }

    public Http2HeadersPayloadFW wrap(boolean padded, boolean priority, DirectBuffer buffer, int offset, int maxLimit)
    {
        this.padded = padded;
        this.priority = priority;
        return wrap(buffer, offset, maxLimit);
    }

    @Override
    public String toString()
    {
        return String.format("HEADERS payload <length=%d>", sizeof());
    }

}

