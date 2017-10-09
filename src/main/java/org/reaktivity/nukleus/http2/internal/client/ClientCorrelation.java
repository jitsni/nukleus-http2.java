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
package org.reaktivity.nukleus.http2.internal.client;

import java.util.Objects;

import org.reaktivity.nukleus.function.MessageConsumer;

public class ClientCorrelation
{
    final long id;
    final Http2ClientConnection http2ClientConnection;
    long acceptCorrelationId;
    String acceptReplyName;
    MessageConsumer acceptReply;
    long acceptReplyStreamId;

    public ClientCorrelation(long id, Http2ClientConnection http2ClientConnection)
    {
        this.id = id;
        this.http2ClientConnection = http2ClientConnection;
    }

    public long id()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        // TODO add other fields
        return Objects.hash(id);
    }

    @Override
    public boolean equals(
            Object obj)
    {
        if (!(obj instanceof ClientCorrelation))
        {
            return false;
        }

        ClientCorrelation that = (ClientCorrelation) obj;
        return this.id == that.id;
    }

    @Override
    public String toString()
    {
        return String.format("[id=%s]",
                id);
    }
}
