package com.gmathur.FileAnalyzer.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

// Immutable task POJO
public class Task implements Serializable {
    private static final long serialVersionUID = 2668728871943496832L;
    public enum TaskStatus {
        PENDING("Task created and submitted to the Executor but not yet running"),
        RUNNING("Task being executed and analysis in progress"),
        ERROR("Task analysis complete with error"),
        DONE("Task analysis completed successfully");

        public final String status;

        private TaskStatus(String status) {
            this.status = status;
        }
    }
    final String taskId;
    final Date createdAt;
    final String fileId;
    TaskStatus taskStatus;
    List<String> taskResult;

    public Task(String taskId, Date createdAt, String fileId, TaskStatus taskStatus) {
        this.taskId = taskId;
        this.createdAt = createdAt;
        this.fileId = fileId;
        this.taskStatus = taskStatus;
        this.taskResult = new ArrayList<>();
    }

    public Task(String taskId, Date createdAt, String fileId, TaskStatus taskStatus, List<String> taskResult) {
        this.taskId = taskId;
        this.createdAt = createdAt;
        this.fileId = fileId;
        this.taskStatus = taskStatus;
        this.taskResult = taskResult;
    }

    public String getTaskId() {
        return taskId;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public String getFileId() {
        return fileId;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public List<String> getTaskResult() {
        return taskResult;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Task{");
        sb.append("taskId='").append(taskId).append('\'');
        sb.append(", createdAt=").append(createdAt);
        sb.append(", fileId='").append(fileId).append('\'');
        sb.append(", taskStatus=").append(taskStatus);
        sb.append(", taskResult=").append(taskResult);
        sb.append('}');
        return sb.toString();
    }

    public static Task updateTaskState(final Task t, final TaskStatus status) {
        return new Task(t.getTaskId(), t.getCreatedAt(), t.getFileId(), status, t.getTaskResult());
    }
}
