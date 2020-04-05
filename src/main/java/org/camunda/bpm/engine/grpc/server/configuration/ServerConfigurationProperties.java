package org.camunda.bpm.engine.grpc.server.configuration;

import jdk.jfr.Description;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

@Data
@Service
@ConfigurationProperties(prefix = "grpc")
@NoArgsConstructor
public class ServerConfigurationProperties {

    @Description("GRPC port")
    private Integer port = 6565;

    @Description("Minimal permitted delay between keep-alive requests in seconds")
    private Integer permitKeepAlive = 1;

    @Description("Delay between keep-alive requests in seconds")
    private Integer keepAlive = 5;

    @Description("Maximal permitted delay between keep-alive requests in seconds")
    private Integer keepAliveTimeout = 60;
}
