package com.gmathur.FileAnalyzer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix="analyzer")
public class AppPropertiesConfig {
    private String resourceServiceFileBaseLocation;
    private Integer threadCorePoolSize;
    private Integer threadMaxPoolSize;
    private Integer threadMaxQueueSize;

    public String getResourceServiceFileBaseLocation() {
        return resourceServiceFileBaseLocation;
    }

    public void setResourceServiceFileBaseLocation(String resourceServiceFileBaseLocation) {
        this.resourceServiceFileBaseLocation = resourceServiceFileBaseLocation;
    }

    public Integer getThreadCorePoolSize() {
        return threadCorePoolSize;
    }

    public void setThreadCorePoolSize(Integer threadCorePoolSize) {
        this.threadCorePoolSize = threadCorePoolSize;
    }

    public Integer getThreadMaxPoolSize() {
        return threadMaxPoolSize;
    }

    public void setThreadMaxPoolSize(Integer threadMaxPoolSize) {
        this.threadMaxPoolSize = threadMaxPoolSize;
    }

    public Integer getThreadMaxQueueSize() {
        return threadMaxQueueSize;
    }

    public void setThreadMaxQueueSize(Integer threadMaxQueueSize) {
        this.threadMaxQueueSize = threadMaxQueueSize;
    }
}
