package com.gmathur.FileAnalyzer.exceptions;

import com.gmathur.FileAnalyzer.models.ExceptionResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.ConnectException;
import java.util.Date;
import java.util.concurrent.RejectedExecutionException;

@ControllerAdvice
@RestController
public class DefaultExceptionHandler extends ResponseEntityExceptionHandler {
    @ExceptionHandler(ConnectException.class)
    public final ResponseEntity<Object> redisConnectionExceptionHandler(Exception ex, WebRequest request) {
        ExceptionResponse r = new ExceptionResponse(new Date(), "Unable to connect to REDIS", ex.getMessage());
        return new ResponseEntity<>(r, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(RejectedExecutionException.class)
    public final ResponseEntity<Object> rejectedExecutionException(Exception ex, WebRequest request) {
        ExceptionResponse r = new ExceptionResponse(new Date(), "Unable to submit request for analysis", ex.getMessage());
        return new ResponseEntity<>(r, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
