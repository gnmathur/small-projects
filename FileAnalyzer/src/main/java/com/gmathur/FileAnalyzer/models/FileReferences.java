package com.gmathur.FileAnalyzer.models;

import java.io.Serializable;
import java.util.Set;

public class FileReferences implements Serializable {
    private static final long serialVersionUID = 3168723451943496832L;
    private String fileIdent;
    private Set<String> fileReferences;

    public FileReferences(String fileIdent, Set<String> fileReferences) {
        this.fileIdent = fileIdent;
        this.fileReferences = fileReferences;
    }

    public String getFileIdent() {
        return fileIdent;
    }

    public void setFileIdent(String fileIdent) {
        this.fileIdent = fileIdent;
    }

    public Set<String> getFileReferences() {
        return fileReferences;
    }

    public void setFileReferences(Set<String> fileReferences) {
        this.fileReferences = fileReferences;
    }
}
