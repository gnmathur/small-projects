package com.gmathur.FileAnalyzer.models;

import java.util.Date;

public class ExceptionResponse {
    private final Date timestamp;
    private final String msg;
    private final String details;

    public ExceptionResponse(Date timestamp, String msg, String details) {
        this.timestamp = timestamp;
        this.msg = msg;
        this.details = details;
    }

    public Date getTimestamp() { return timestamp; }
    public String getMsg() { return msg; }
    public String getDetails() { return details; }
}
