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
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.GO_AWAY;

/*

    Flyweight for HTTP2 GOAWAY frame's payload

    +=+=============+===============================================+
    |R|                  Last-Stream-ID (31)                        |
    +-+-------------------------------------------------------------+
    |                      Error Code (32)                          |
    +---------------------------------------------------------------+
    |                  Additional Debug Data (*)                    |
    +---------------------------------------------------------------+

 */
public class Http2GoawayPayloadFW extends Flyweight
{
    private static final int ERROR_CODE_OFFSET = 4;


    public int lastStreamId()
    {
        return buffer().getInt(offset(), BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    public int errorCode()
    {
        return buffer().getInt(offset() + ERROR_CODE_OFFSET, BIG_ENDIAN);
    }

    @Override
    public int limit() {
        return maxLimit();
    }

    @Override
    public Http2GoawayPayloadFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("GOAWAY <length=%d>", sizeof());
    }

}

