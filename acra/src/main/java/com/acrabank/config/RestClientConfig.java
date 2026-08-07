package com.acrabank.config;

import java.net.http.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class RestClientConfig {

    /**
     * Both timeouts are set explicitly because the defaults are "wait forever". A government API that accepts a
     * connection and then never answers would otherwise pin a request thread indefinitely - the same failure mode as an
     * unbounded pool wait, and just as invisible in the logs.
     *
     * <p>No {@code defaultStatusHandler} is registered here on purpose. RestClient's default is to throw on any
     * 4xx/5xx, but {@link com.acrabank.client.AcraProfileClient} has to *see* a 401 in order to drop its cached token
     * and retry, so status inspection stays at the call site.
     */
    @Bean
    public RestClient acraRestClient(RestClient.Builder builder, AcraProperties properties) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(properties.requestTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(properties.requestTimeout());

        return builder.baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }
}
