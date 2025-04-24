package com.gmathur.FileAnalyzer.service;

import com.gmathur.FileAnalyzer.AppPropertiesConfig;
import com.gmathur.FileAnalyzer.exceptions.FileServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class FileResourceService implements ResourceService {
    private final AppPropertiesConfig appPropertiesConfig;
    @Override
    public void init() { }

    @Autowired
    FileResourceService(AppPropertiesConfig appPropertiesConfig) {
        this.appPropertiesConfig = appPropertiesConfig;
    }

    @Override
    public InputStream getResource(final String fileName) {
        Path p = Paths.get(appPropertiesConfig.getResourceServiceFileBaseLocation() + "/" + fileName);
        try {
            return new FileSystemResource(p).getInputStream();
        } catch (IOException e) {
            throw new FileServiceException("Cannot read " + fileName + " from the filesystem");
        }
    }
}
