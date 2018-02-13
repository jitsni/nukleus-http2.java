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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;

import java.util.Deque;
import java.util.LinkedList;

class Http2Stream
{
    final Http2Connection connection;
    final HttpWriteScheduler httpWriteScheduler;
    final int http2StreamId;
    final int maxHeaderSize;
    final long targetId;
    final long correlationId;
    Http2Connection.State state;
    long http2OutWindow;
    long applicationReplyBudget;
    long http2InWindow;

    long contentLength;
    long totalData;

    Deque<WriteScheduler.Entry> replyQueue = new LinkedList<>();
    boolean endStream;

    long totalOutData;
    private ServerStreamFactory factory;

    MessageConsumer applicationReplyThrottle;
    long applicationReplyId;

    Http2Stream(ServerStreamFactory factory, Http2Connection connection, int http2StreamId, Http2Connection.State state,
                MessageConsumer applicationTarget, HttpWriter httpWriter)
    {
        this.factory = factory;
        this.connection = connection;
        this.http2StreamId = http2StreamId;
        this.targetId = factory.supplyStreamId.getAsLong();
        this.correlationId = factory.supplyCorrelationId.getAsLong();
        this.http2InWindow = connection.localSettings.initialWindowSize;

        this.http2OutWindow = connection.remoteSettings.initialWindowSize;
        this.state = state;
        this.httpWriteScheduler = new HttpWriteScheduler(factory.memoryManager, applicationTarget, httpWriter, targetId, this);
        // Setting the overhead to zero for now. Doesn't help when multiple streams are in picture
        this.maxHeaderSize = 0;
    }

    boolean isClientInitiated()
    {
        return http2StreamId%2 == 1;
    }

    void onHttpEnd()
    {
        connection.writeScheduler.dataEos(http2StreamId);
        applicationReplyThrottle = null;
    }

    void onHttpAbort()
    {
        // more request data to be sent, so send ABORT
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            httpWriteScheduler.doAbort();
        }

        connection.writeScheduler.rst(http2StreamId, Http2ErrorCode.CONNECT_ERROR);

        connection.closeStream(this);
    }

    void onHttpReset()
    {
        // reset the response stream
        if (applicationReplyThrottle != null)
        {
            factory.doReset(applicationReplyThrottle, applicationReplyId);
        }

        connection.writeScheduler.rst(http2StreamId, Http2ErrorCode.CONNECT_ERROR);

        connection.closeStream(this);
    }

    public void onPayloadRegion(long address, int length, long regionStreamId) {
        httpWriteScheduler.onPayloadRegion(address, length, regionStreamId);
    }

    void onData()
    {
        boolean written = httpWriteScheduler.onData(factory.http2DataRO);
        assert written;
    }

    void onAbort()
    {
        // more request data to be sent, so send ABORT
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            httpWriteScheduler.doAbort();
        }

        // reset the response stream
        if (applicationReplyThrottle != null)
        {
            factory.doReset(applicationReplyThrottle, applicationReplyId);
        }

        close();
    }

    void onReset()
    {
        // more request data to be sent, so send ABORT
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            httpWriteScheduler.doAbort();
        }

        // reset the response stream
        if (applicationReplyThrottle != null)
        {
            factory.doReset(applicationReplyThrottle, applicationReplyId);
        }

        close();
    }

    void onEnd()
    {
        // more request data to be sent, so send ABORT
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            httpWriteScheduler.doAbort();
        }

        if (applicationReplyThrottle != null)
        {
            factory.doReset(applicationReplyThrottle, applicationReplyId);
        }

        close();
    }

    void onThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
//        switch (msgTypeId)
//        {
//            case WindowFW.TYPE_ID:
//                if (isClientInitiated())
//                {
//                    factory.windowRO.wrap(buffer, index, index + length);
//                    int credit = factory.windowRO.credit();
//                    int padding = factory.windowRO.padding();
//                    long groupId = factory.windowRO.groupId();
//
//                    httpWriteScheduler.onWindow(credit, padding, groupId);
//                }
//                break;
//            case ResetFW.TYPE_ID:
//                onHttpReset();
//                break;
//            default:
//                // ignore
//                break;
//        }
    }

    void close()
    {
        httpWriteScheduler.onReset();
    }

    void sendHttpWindow()
    {
//        // buffer may already have some data, so can only send window for remaining
//        int occupied = replyBuffer == null ? 0 : replyBuffer.size();
//        long maxWindow = Math.min(http2OutWindow, connection.factory.bufferPool.slotCapacity() - occupied);
//        long applicationReplyCredit = maxWindow - applicationReplyBudget;
//        if (applicationReplyCredit > 0)
//        {
//            applicationReplyBudget += applicationReplyCredit;
//            int applicationReplyPadding = connection.networkReplyPadding + maxHeaderSize;
//            connection.factory.doWindow(applicationReplyThrottle, applicationReplyId,
//                    (int) applicationReplyCredit, applicationReplyPadding, connection.networkReplyGroupId);
//        }
    }


}
