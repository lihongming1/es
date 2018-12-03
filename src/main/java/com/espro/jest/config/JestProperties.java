package com.espro.jest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ConfigurationProperties(prefix = "spring.elasticsearch.jest")
public class JestProperties {

    private List<String> uris = new ArrayList<String>(
            Collections.singletonList("http://localhost:9200"));

}
