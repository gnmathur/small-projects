package com.gmathur.FileAnalyzer.repository;

import com.gmathur.FileAnalyzer.models.FileReferences;
import com.gmathur.FileAnalyzer.models.Task;

import java.util.Date;
import java.util.List;

public interface RedisRepository {

    void addTask(Task task);

    void addTaskWithReferencesAndCreateTime(Task task);

    Task getTask(String id);

    void addReferences(String fileId, FileReferences refs);

    FileReferences getFileReferences(String id);

    void updateReferences(String reference, List<String> fileIds);

    void addFileCreationTime(String fileId, Date date);

    Date getFileCreationTime(String fileId);
}
