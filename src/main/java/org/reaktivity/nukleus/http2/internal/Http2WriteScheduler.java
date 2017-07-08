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
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.IntConsumer;

import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.DATA;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.GO_AWAY;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PUSH_PROMISE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.RST_STREAM;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.WINDOW_UPDATE;

public class Http2WriteScheduler implements WriteScheduler
{
    private final Http2Connection connection;
    private static final IntConsumer NOOP = x -> {};

    private final MutableDirectBuffer read = new UnsafeBuffer(new byte[0]);
    private final MutableDirectBuffer write = new UnsafeBuffer(new byte[0]);

    private final Http2Writer http2Writer;
    private final NukleusWriteScheduler writer;
    private Deque<Entry> replyQueue;

    private boolean end;
    private boolean endSent;
    private int entryCount;

    Http2WriteScheduler(
            Http2Connection connection,
            long sourceOutputEstId,
            Slab slab,
            MessageConsumer networkConsumer,
            Http2Writer http2Writer,
            long targetId)
    {
        this.connection = connection;
        this.http2Writer = http2Writer;
        this.writer = new NukleusWriteScheduler(connection, sourceOutputEstId, slab, networkConsumer, http2Writer, targetId);
        this.replyQueue = new LinkedList<>();
    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 window size increment
        Http2FrameType type = WINDOW_UPDATE;
        IntConsumer progress = NOOP;
        Http2Stream stream = stream(streamId);
        Flyweight.Builder.Visitor visitor = http2Writer.visitWindowUpdate(streamId, update);

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            http2(stream, type, sizeof, visitor, progress);
        }
        else
        {
            WindowEntry entry = new WindowEntry(stream, streamId, sizeof, type, visitor, progress, update);
            addEntry(entry);
        }
        return true;
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        int streamId = 0;
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for a ping
        IntConsumer progress = NOOP;
        Http2Stream stream = null;
        Http2FrameType type = PING;

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            Flyweight.Builder.Visitor visitor = http2Writer.visitPingAck(buffer, offset, length);
            http2(stream, type, sizeof, visitor, progress);
        }
        else
        {
            MutableDirectBuffer copy = new UnsafeBuffer(new byte[8]);
            copy.putBytes(0, buffer, offset, length);
            Flyweight.Builder.Visitor visitor = http2Writer.visitPingAck(copy, 0, length);
            PingAckEntry entry = new PingAckEntry(stream, streamId, sizeof, type, visitor, progress);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        int streamId = 0;
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for goaway payload
        Http2Stream stream = null;
        Flyweight.Builder.Visitor goaway = http2Writer.visitGoaway(lastStreamId, errorCode);
        Http2FrameType type = GO_AWAY;
        IntConsumer progress = NOOP;

        if (!buffered(0) && sizeof <= connection.outWindow)
        {
            http2(stream, type, sizeof, goaway, progress);
        }
        else
        {
            GoAwayEntry entry = new GoAwayEntry(stream, streamId, sizeof, type, goaway, progress, lastStreamId, errorCode);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 for RST_STREAM payload
        Flyweight.Builder.Visitor visitor = http2Writer.visitRst(streamId, errorCode);
        Http2Stream stream = stream(streamId);
        Http2FrameType type = RST_STREAM;
        IntConsumer progress = NOOP;

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            http2(stream, type, sizeof, visitor, progress);
        }
        else
        {
            RstEntry entry = new RstEntry(stream, streamId, sizeof, type, visitor, progress, errorCode);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        int streamId = 0;
        int sizeof = 9 + 6;             // +9 for HTTP2 framing, +6 for a setting
        Flyweight.Builder.Visitor settings = http2Writer.visitSettings(maxConcurrentStreams);
        Http2FrameType type = SETTINGS;
        IntConsumer progress = NOOP;
        Http2Stream stream = null;

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            http2(stream, type, sizeof, settings, progress);
        }
        else
        {
            SettingsEntry entry = new SettingsEntry(stream, streamId, sizeof, type, settings, progress, maxConcurrentStreams);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean settingsAck()
    {
        int streamId = 0;
        int sizeof = 9;                 // +9 for HTTP2 framing
        Flyweight.Builder.Visitor visitor = http2Writer.visitSettingsAck();
        Http2FrameType type = SETTINGS;
        IntConsumer progress = NOOP;
        Http2Stream stream = null;

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            http2(stream, type, sizeof, visitor, progress);
        }
        else
        {
            SettingsAckEntry entry = new SettingsAckEntry(stream, streamId, sizeof, type, visitor, progress);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers)
    {
        int sizeof = 9 + headersLength(headers);    // +9 for HTTP2 framing
        Http2FrameType type = HEADERS;
        IntConsumer progress = NOOP;
        Http2Stream stream = stream(streamId);

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            Flyweight.Builder.Visitor visitor = http2Writer.visitHeaders(streamId, headers, connection::mapHeaders);
            http2(stream, type, sizeof, visitor, progress);
        }
        else
        {
            MutableDirectBuffer copy = new UnsafeBuffer(new byte[8192]);        // TODO
            HpackHeaderBlockFW.Builder blockRW = new HpackHeaderBlockFW.Builder();  // TODO
            blockRW.wrap(copy, 0, copy.capacity());
            connection.mapHeaders(headers, blockRW);

            Flyweight.Builder.Visitor visitor = http2Writer.visitHeaders(streamId, copy, 0, copy.capacity());
            HeadersEntry entry = new HeadersEntry(stream, streamId, sizeof, type, visitor, progress);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               IntConsumer progress)
    {
        int sizeof = 9 + 4 + headersLength(headers);    // +9 for HTTP2 framing, +4 for promised stream id
        Http2FrameType type = PUSH_PROMISE;
        Http2Stream stream = stream(streamId);

        if (!buffered(streamId) && sizeof <= connection.outWindow)
        {
            Flyweight.Builder.Visitor pushPromise =
                    http2Writer.visitPushPromise(streamId, promisedStreamId, headers, connection::mapPushPromize);
            http2(stream, type, sizeof, pushPromise, progress);
        }
        else
        {
            MutableDirectBuffer copy = new UnsafeBuffer(new byte[8192]);
            HpackHeaderBlockFW.Builder blockRW = new HpackHeaderBlockFW.Builder();  // TODO
            blockRW.wrap(copy, 0, copy.capacity());
            connection.mapHeaders(headers, blockRW);

            Flyweight.Builder.Visitor visitor =
                    http2Writer.visitPushPromise(streamId, promisedStreamId, copy, 0, copy.capacity());

            PushPromiseEntry entry = new PushPromiseEntry(stream, streamId, sizeof, type, visitor, progress);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length, IntConsumer progress)
    {
        assert length > 0;
        assert streamId != 0;

        int sizeof = 9 + length;    // +9 for HTTP2 framing
        Http2FrameType type = DATA;
        Http2Stream stream = stream(streamId);
        if (stream == null)
        {
            return true;
        }

        if (!buffered(0) && !buffered(streamId) && sizeof <= connection.outWindow && length <= connection.http2OutWindow &&
                length <= stream.http2OutWindow && length <= connection.remoteSettings.maxFrameSize)
        {
            Flyweight.Builder.Visitor data = http2Writer.visitData(streamId, buffer, offset, length);
            http2(stream, type, sizeof, data, progress);
        }
        else
        {
            MutableDirectBuffer replyBuffer = stream.acquireReplyBuffer(this::write);
            CircularDirectBuffer cdb = stream.replyBuffer;

            // Store as two contiguous parts (as it is circular buffer)
            int part1 = cdb.writeContiguous(replyBuffer, buffer, offset, length);
            assert part1 > 0;
            Flyweight.Builder.Visitor data1 = http2Writer.visitData(streamId, buffer, offset, part1);
            DataEntry entry1 = new DataEntry(stream, streamId, type, part1 + 9, data1, progress);
            addEntry(entry1);

            int part2 = length - part1;
            if (part2 > 0)
            {
                part2 = cdb.writeContiguous(replyBuffer, buffer, offset, part2);
                assert part2 > 0;
                assert part1 + part2 == length;
                Flyweight.Builder.Visitor data2 = http2Writer.visitData(streamId, buffer, offset, part2);
                DataEntry entry2 = new DataEntry(stream, streamId, type, part2 + 9, data2, progress);
                addEntry(entry2);
            }
            flush();
        }
        return true;
    }

    @Override
    public boolean dataEos(int streamId)
    {
        int sizeof = 9;    // +9 for HTTP2 framing
        Flyweight.Builder.Visitor data = http2Writer.visitDataEos(streamId);
        Http2FrameType type = DATA;
        IntConsumer progress = NOOP;

        Http2Stream stream = connection.http2Streams.get(streamId);
        if (stream == null)
        {
            return true;
        }
        stream.endStream = true;

        if (!buffered(streamId) && sizeof <= connection.outWindow && 0 <= connection.http2OutWindow &&
                0 <= stream.http2OutWindow)
        {
            http2(stream, type, sizeof, data, progress);
            connection.closeStream(stream);
        }
        else
        {
            DataEosEntry entry = new DataEosEntry(stream, streamId, sizeof, type, data, progress);
            addEntry(entry);
        }

        return true;
    }

    private void addEntry(Entry entry)
    {
        Deque queue = queue(entry.stream);
        if (queue != null)
        {
            queue.add(entry);
        }
    }

    // Since it is not encoding, this gives an approximate length of header block
    private int headersLength(ListFW<HttpHeaderFW> headers)
    {
        int[] length = new int[1];
        headers.forEach(x -> length[0] += x.name().sizeof() + x.value().sizeof() + 4);
        return length[0];
    }

    @Override
    public void doEnd()
    {
        end = true;

        assert entryCount >= 0;
        if (entryCount == 0 && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    void flush()
    {
        if (connection.outWindow < connection.outWindowThreshold)
        {
            // Instead of sending small updates, wait until a bigger window accumulates
            return;
        }

        Entry entry;
        while ((entry = pop()) != null)
        {
            entry.write();

            if (!buffered(entry.stream) && entry.stream != null)
            {
                entry.stream.releaseReplyBuffer();
            }
        }
        writer.flush();

        if (entryCount == 0 && end && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    @Override
    public void onHttp2Window()
    {
        flush();
    }

    @Override
    public void onHttp2Window(int streamId)
    {
        flush();
    }

    @Override
    public void onWindow()
    {
        flush();
    }

    private Entry pop()
    {
        if (buffered((Http2Stream)null))
        {
            // There are entries on streamId=0 but cannot make progress, so
            // not make any progress on other streams
            return pop((Http2Stream)null);
        }

        // TODO Map#values may not iterate randomly, randomly pick a stream ??
        // Select a frame on a HTTP2 stream that can be written
        for(Http2Stream stream : connection.http2Streams.values())
        {
            Entry entry = pop(stream);
            if (entry != null)
            {
                return entry;
            }
        }

        return null;
    }

    private Entry pop(Http2Stream stream)
    {
        if (buffered(stream))
        {
            Deque replyQueue = queue(stream);
            Entry entry = (Entry) replyQueue.peek();
            if (entry.fits())
            {
                entryCount--;
                entry =  (Entry) replyQueue.poll();
                return entry;
            }
        }

        return null;
    }

    private MutableDirectBuffer read(MutableDirectBuffer buffer)
    {
        read.wrap(buffer.addressOffset(), buffer.capacity());
        return read;
    }

    private MutableDirectBuffer write(MutableDirectBuffer buffer)
    {
        write.wrap(buffer.addressOffset(), buffer.capacity());
        return write;
    }

    private Deque queue(Http2Stream stream)
    {
        return (stream == null) ? replyQueue : stream.replyQueue;
    }

    private boolean buffered(Http2Stream stream)
    {
        Deque queue = (stream == null) ? replyQueue : stream.replyQueue;
        return buffered(queue);
    }

    private boolean buffered(Deque queue)
    {
        return !(queue == null || queue.isEmpty());
    }

    CircularDirectBuffer buffer(int streamId)
    {
        assert streamId != 0;

        Http2Stream stream = connection.http2Streams.get(streamId);
        return stream.replyBuffer;
    }


    boolean buffered(int streamId)
    {
        if (streamId == 0)
        {
            return !(replyQueue == null || replyQueue.isEmpty());
        }
        else
        {
            Http2Stream stream = connection.http2Streams.get(streamId);
            return !(stream == null || stream.replyQueue == null || stream.replyQueue.isEmpty());
        }
    }

    Http2Stream stream(int streamId)
    {
        return streamId == 0 ? null : connection.http2Streams.get(streamId);
    }

    private void http2(Http2Stream stream, Http2FrameType type,
                       int lengthGuess, Flyweight.Builder.Visitor visitor, IntConsumer progress, boolean flush)
    {
        int sizeof = writer.http2Frame(lengthGuess, visitor);
        assert sizeof >= 9;
        assert connection.outWindow >= sizeof;

        connection.outWindow -= sizeof;

        int length = sizeof - 9;
        progress.accept(length);
        if (type == DATA)
        {
            stream.http2OutWindow -= length;
            connection.http2OutWindow -= length;
            stream.totalOutData += length;
        }
        if (flush)
        {
            writer.flush();
        }

    }

    private void http2(Http2Stream stream, Http2FrameType type,
                       int lengthGuess, Flyweight.Builder.Visitor visitor, IntConsumer progress)
    {
        http2(stream, type, lengthGuess, visitor, progress, true);
    }

    private class Entry
    {
        final int streamId;
        final int sizeof;
        final Http2FrameType type;
        final Flyweight.Builder.Visitor visitor;
        final IntConsumer progress;
        final Http2Stream stream;

        Entry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
              Flyweight.Builder.Visitor visitor, IntConsumer progress)
        {
            assert sizeof >= 9;

            this.stream = stream;
            this.streamId = streamId;
            this.sizeof = sizeof;
            this.type = type;
            this.visitor = visitor;
            this.progress = progress;

            entryCount++;
        }

        boolean fits()
        {
            return sizeof <= connection.outWindow;
        }

        void write()
        {
            http2(stream, type, sizeof, visitor, progress, false);
        }

    }

    private class WindowEntry extends Entry
    {
        private final int update;

        WindowEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                    Flyweight.Builder.Visitor visitor, IntConsumer progress, int update)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
            this.update = update;
        }
    }

    private class PingAckEntry extends Entry
    {
        PingAckEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                     Flyweight.Builder.Visitor visitor, IntConsumer progress)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class GoAwayEntry extends Entry
    {
        GoAwayEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                    Flyweight.Builder.Visitor visitor, IntConsumer progress,
                    int lastStreamId, Http2ErrorCode errorCode)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class RstEntry extends Entry
    {
        RstEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                 Flyweight.Builder.Visitor visitor, IntConsumer progress,
                 Http2ErrorCode errorCode)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class SettingsEntry extends Entry
    {
        SettingsEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                      Flyweight.Builder.Visitor visitor, IntConsumer progress, int maxConcurrentStream)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class SettingsAckEntry extends Entry
    {
        SettingsAckEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                         Flyweight.Builder.Visitor visitor, IntConsumer progress)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class HeadersEntry extends Entry
    {
        HeadersEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                     Flyweight.Builder.Visitor visitor, IntConsumer progress)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class PushPromiseEntry extends Entry
    {
        PushPromiseEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                         Flyweight.Builder.Visitor visitor, IntConsumer progress)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }
    }

    private class DataEosEntry extends Entry
    {
        DataEosEntry(Http2Stream stream, int streamId, int sizeof, Http2FrameType type,
                     Flyweight.Builder.Visitor visitor, IntConsumer progress)
        {
            super(stream, streamId, sizeof, type, visitor, progress);
        }

        @Override
        void write()
        {
            super.write();
            connection.closeStream(stream);
        }
    }

    private class DataEntry extends Entry
    {
        final int length;

        DataEntry(
                Http2Stream stream,
                int streamId,
                Http2FrameType type,
                int sizeof,
                Flyweight.Builder.Visitor visitor,
                IntConsumer progress)
        {
            super(stream, streamId, sizeof, type, visitor, progress);

            assert streamId != 0;
            length = sizeof - 9;
        }

        boolean fits()
        {
            // limit by nuklei window, http2 windows, peer's max frame size
            int min = Math.min((int) connection.http2OutWindow, (int) stream.http2OutWindow);
            min = Math.min(min, length);
            min = Math.min(min, connection.remoteSettings.maxFrameSize);
            min = Math.min(min, connection.outWindow - 9);

            if (min > 0)
            {
                int remaining = length - min;
                if (remaining > 0)
                {
                    entryCount--;
                    stream.replyQueue.poll();
                    DataEntry entry1 = new DataEntry(stream, streamId, type, min + 9, visitor, progress);
                    DataEntry entry2 = new DataEntry(stream, streamId, type, remaining + 9, visitor, progress);

                    stream.replyQueue.addFirst(entry2);
                    stream.replyQueue.addFirst(entry1);
                }
            }

            return min > 0;
        }

        @Override
        void write()
        {
            DirectBuffer read = stream.acquireReplyBuffer(Http2WriteScheduler.this::read);
            int offset = stream.replyBuffer.readOffset();
            int readLength = stream.replyBuffer.read(length);
            assert readLength == length;
            Flyweight.Builder.Visitor visitor = http2Writer.visitData(streamId, read, offset, readLength);
            http2(stream, type, readLength, visitor, progress, false);
        }

        public String toString()
        {
            return String.format("length=%d", length);
        }

    }

}