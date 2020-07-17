package org.camunda.bpm.engine.grpc.server.configuration;

import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiators;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.grpc.server.service.GrpcService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
@ComponentScan("org.camunda.bpm.engine.grpc.server")
public class ServerConfiguration {

    @Bean
    public Server getGrpcServer(
        GrpcService grpcService,
        ServerConfigurationProperties properties,
        List<ServerInterceptor> serverInterceptors
    ) throws IOException {
        log.info("Starting GRPC server on port '{}'...", properties.getPort());

        NettyServerBuilder builder = NettyServerBuilder
            .forPort(properties.getPort())
            .addService(grpcService)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(properties.getPermitKeepAlive(), TimeUnit.SECONDS)
            .keepAliveTime(properties.getKeepAlive(), TimeUnit.SECONDS)
            .keepAliveTimeout(properties.getKeepAliveTimeout(), TimeUnit.SECONDS)
            .protocolNegotiator(InternalProtocolNegotiators.plaintext());

        serverInterceptors.forEach(builder::intercept);

        final Server server = builder.build();

        server.start();
        log.info("GRPC server started successfully on port '{}'", properties.getPort());

        return server;
    }
}
