package com.gmathur.FileAnalyzer.models;

import java.util.Date;

public class FileCreationTime {
    private static final long serialVersionUID = 716872349911196731L;
    private String fileId;
    private Date date;

    public FileCreationTime(String fileId, Date date) {
        this.fileId = fileId;
        this.date = date;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
