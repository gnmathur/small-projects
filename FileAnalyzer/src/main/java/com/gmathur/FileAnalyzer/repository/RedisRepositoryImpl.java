package com.gmathur.FileAnalyzer.repository;

import com.gmathur.FileAnalyzer.models.FileReferences;
import com.gmathur.FileAnalyzer.models.Task;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Spring repository API for interacting with REDIS. This module understands both the domain models and the business
 * logic. All operations are transactional. Transactions on REDIS should guarantee serialized updates across concurrent
 * access to REDIS from multiple threads and processes
 */
@Repository
@Transactional
public class RedisRepositoryImpl implements RedisRepository {
    private final RedisTemplate<String, Object> redisTemplate;
    private HashOperations<String, String, Task> taskHashOperations;
    private HashOperations<String, String, FileReferences> fileRefsHashOperations;
    private HashOperations<String, String, Date> fileCreateHashOperations;
    private final String TASK_TBL_IDENT = "TASK";
    private final String FILEID_REF_TBL_IDENT = "FILE_ID_REF";
    private final String FILE_CREATE_TBL_IDENT = "FILE_CREATE";

    RedisRepositoryImpl(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        this.taskHashOperations = redisTemplate.opsForHash();
        this.fileRefsHashOperations = redisTemplate.opsForHash();
        this.fileCreateHashOperations = redisTemplate.opsForHash();
    }

    /**
     * Add a task to the task HSET
     *
     * @param task Task data model to update
     */
    @Override
    public void addTask(final Task task) {
        taskHashOperations.put(TASK_TBL_IDENT, task.getTaskId(), task);
    }

    /**
     * Add a task to the task HSET. Also, Extract task information to update the file id cross reference HSET and task
     * create time HSET
     *
     * @param id Task identifier (UUID as string)
     * @param task Task data model to update
     */
    @Override
    public void addTaskWithReferencesAndCreateTime(final Task task) {
        addTask(task);
        updateReferences(task.getFileId(), task.getTaskResult());
        addFileCreationTime(task.getFileId(), task.getCreatedAt());
    }

    @Override
    public Task getTask(final String id) {
        return taskHashOperations.get(TASK_TBL_IDENT, id);
    }

    @Override
    public void addReferences(String fileId, FileReferences refs) {
        fileRefsHashOperations.put(FILEID_REF_TBL_IDENT, fileId, refs);
    }

    @Override
    public FileReferences getFileReferences(String id) {
        return fileRefsHashOperations.get(FILEID_REF_TBL_IDENT, id);
    }

    /**
     * Add fileId as a reference to the list of file references maintained for each of the file ids in fRefs
     *
     * @param reference File identifier to be added as a reference
     * @param fileIds List of file identifiers for which <fieldId> needs to be added as a reference
     */
    @Override
    public void updateReferences(String reference, List<String> fileIds) {
        for (String fRef: fileIds) {
            FileReferences fileReferences = getFileReferences(fRef);
            if (fileReferences == null) {
                fileReferences = new FileReferences(fRef, new HashSet<>());
            }
            fileReferences.getFileReferences().add(reference);
            addReferences(fRef, fileReferences);
        }
    }

    @Override
    public void addFileCreationTime(String fileId, Date date) {
        fileCreateHashOperations.put(FILE_CREATE_TBL_IDENT, fileId, date);
    }

    @Override
    public Date getFileCreationTime(String fileId) {
        return fileCreateHashOperations.get(FILE_CREATE_TBL_IDENT, fileId);
    }
}
