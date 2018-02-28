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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.RST_STREAM;

/*

    Flyweight for HTTP2 RST_STREAM frame's payload

    +=+=============+===============================================+
    |                        Error Code (32)                        |
    +---------------------------------------------------------------+

 */
public class Http2RstStreamPayloadFW extends Flyweight
{

    public int errorCode()
    {
        return buffer().getInt(offset(), BIG_ENDIAN);
    }

    @Override
    public int limit() {
        return maxLimit();
    }

    @Override
    public Http2RstStreamPayloadFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int payloadLength = sizeof();
        if (payloadLength != 4)
        {
            throw new IllegalArgumentException(String.format("Invalid RST_STREAM frame length=%d (must be 4)", payloadLength));
        }
        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("RST_STREAM payload <length=%d>", sizeof());
    }

}

