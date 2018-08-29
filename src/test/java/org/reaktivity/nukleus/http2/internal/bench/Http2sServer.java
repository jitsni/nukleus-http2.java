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
package org.reaktivity.nukleus.http2.internal.bench;

/*
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.http2.internal.Http2Controller;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.nukleus.tls.internal.TlsController;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;*/

public class Http2sServer
{

    public static void main(String... args) throws Exception
    {/*
        Properties properties = new Properties();
        properties.setProperty(ReaktorConfiguration.DIRECTORY_PROPERTY_NAME, "target/nukleus-benchmarks");
        //properties.setProperty(ReaktorConfiguration.ABORT_STREAM_FRAME_TYPE_ID, "3");
        //properties.setProperty(ReaktorConfiguration.STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, Long.toString(1024L * 1024L * 128L));
        //properties.setProperty(ReaktorConfiguration.BUFFER_POOL_CAPACITY_PROPERTY, Long.toString(1024L * 1024L * 128L));

        // cp tls-keys/java/*.jks target/nukleus-benchmarks/tls/
        // set system properties
        //
//        properties.setProperty("tls.keystore", "keystore.jks");
//        properties.setProperty("tls.keystore.password", "change-this-password");
//        properties.setProperty("tls.truststore", "truststore.jks");
//        properties.setProperty("tls.truststore.password", "change-this-password");


        Predicate<Class<? extends Controller>> matcher =
                c -> Http2Controller.class.isAssignableFrom(c) || TcpController.class.isAssignableFrom(c) ||
                        TlsController.class.isAssignableFrom(c);
        final Configuration configuration = new Configuration(properties);
        Reaktor reaktor = Reaktor.builder()
                                 .config(configuration)
                                 .nukleus(n -> "tcp".equals(n) || "tls".equals(n) || "http2".equals(n))
                                 .controller(matcher)
                                 .errorHandler(Throwable::printStackTrace)
                                 .build();

        TcpController tcpController = reaktor.controller(TcpController.class);
        TlsController tlsController = reaktor.controller(TlsController.class);
        Http2Controller http2Controller = reaktor.controller(Http2Controller.class);
        reaktor.start();
        Map<String, String> headers = new HashMap<>();
        headers.put(":authority", "origin-server.example.com:8080");
        long http2In = http2Controller.routeServer("tls", 0L, "http2", 0L, headers).get();
        long tlsIn = tlsController.routeServer("tcp", 0L, "http2", http2In, "origin-server.example.com", "h2").get();
        //tcpController.routeServer("any", 8080, "tls", tlsIn, getByName("127.0.0.1")).get();
        tcpController.routeServer("localhost", 8080, "tls", tlsIn).get();


        Thread.sleep(10000000);*/
    }

}
