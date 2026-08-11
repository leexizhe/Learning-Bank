package com.acra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {

    @Bean
    public RestClient acraRestClient(RestClient.Builder builder, AcraProperties properties) {
        return builder.baseUrl(properties.baseUrl()).build();
    }
}
