package com.gmathur.FileAnalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {
    private static final Logger logger = LoggerFactory.getLogger(AsyncConfig.class);

    private final AppPropertiesConfig appPropertiesConfig;

    @Autowired
    AsyncConfig(AppPropertiesConfig appProps) {
        this.appPropertiesConfig = appProps;
    }

    @Bean(name = "taskProcessorExecutor")
    public Executor taskProcessorExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(appPropertiesConfig.getThreadCorePoolSize());
        executor.setMaxPoolSize(appPropertiesConfig.getThreadMaxPoolSize());
        executor.setQueueCapacity(appPropertiesConfig.getThreadMaxQueueSize());
        executor.setThreadNamePrefix("Task Processor");
        executor.initialize();

        logger.info("Initialized thread pool (" +
                "max: " + appPropertiesConfig.getThreadMaxPoolSize() +
                " core: " + appPropertiesConfig.getThreadCorePoolSize() +
                " queueMax: " + appPropertiesConfig.getThreadMaxQueueSize() + ")");
        return executor;
    }
}
