package com.amazonaws.services.kinesis.dropwizard;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;
import com.amazonaws.services.kinesis.log4j.AppenderConstants;
import com.amazonaws.services.kinesis.logback.KinesisAppender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.AppenderFactory;

import javax.validation.constraints.NotNull;

@JsonTypeName("kinesis")
public class KinesisAppenderFactory implements AppenderFactory {

    @NotNull
    @JsonProperty
    private String streamName;

    @NotNull
    @JsonProperty
    private String endpoint;

    @NotNull
    @JsonProperty
    private String region;

    @JsonProperty
    private String encoding = AppenderConstants.DEFAULT_ENCODING;

    @JsonProperty
    private int maxRetries = AppenderConstants.DEFAULT_MAX_RETRY_COUNT;

    @JsonProperty
    private int bufferSize = AppenderConstants.DEFAULT_BUFFER_SIZE;

    @JsonProperty
    private int threadCount = AppenderConstants.DEFAULT_THREAD_COUNT;

    @JsonProperty
    private int shutdownTimer = AppenderConstants.DEFAULT_SHUTDOWN_TIMEOUT_SEC;

    @JsonProperty
    private String pattern = "%m";

    @Override
    public Appender<ILoggingEvent> build(LoggerContext context, String applicationName, Layout<ILoggingEvent> layout) {
        KinesisAppender appender = new KinesisAppender();
        appender.setContext(context);
        appender.setName("dropwizard-kinesis");
        appender.setStreamName(streamName);
        appender.setEndpoint(endpoint);
        appender.setRegion(region);
        appender.setEncoding(encoding);
        appender.setMaxRetries(maxRetries);
        appender.setBufferSize(bufferSize);
        appender.setThreadCount(threadCount);
        appender.setShutdownTimeout(shutdownTimer);
        appender.setLayout(layout == null ? buildLayout() : (PatternLayout) layout);

        return appender;
    }

    private PatternLayout buildLayout() {
        PatternLayout patternLayout = new PatternLayout();
        patternLayout.setPattern(pattern);
        return patternLayout;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void setShutdownTimer(int shutdownTimer) {
        this.shutdownTimer = shutdownTimer;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
}
