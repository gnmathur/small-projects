package com.gmathur.FileAnalyzer.service;

import java.io.InputStream;

// Implementations are expected to hold no state because the ResourceService is expected to be used from
// the task context
public interface ResourceService {
    /**
     * Initialize access to the resources. For S3 resources, as an example, this routine could be used to setup
     * the transport to the S3 service using the AWS secrets
     */
    public void init();

    /**
     * Read resource byte-by-byte. Caller can wrap it under Buffered readers if they need to process line-by-line
     * @param resourceIdent Resource to read
     * @return An InputStream to the resource
     */
    InputStream getResource(final String resourceIdent);
}
