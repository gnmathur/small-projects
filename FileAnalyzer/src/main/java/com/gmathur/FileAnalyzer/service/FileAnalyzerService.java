package com.gmathur.FileAnalyzer.service;

import com.gmathur.FileAnalyzer.exceptions.FileServiceException;
import com.gmathur.FileAnalyzer.models.FileReferences;
import com.gmathur.FileAnalyzer.models.Task;
import com.gmathur.FileAnalyzer.repository.RedisRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class FileAnalyzerService {
    private static final Logger logger = LoggerFactory.getLogger(FileAnalyzerService.class);
    private final FileResourceService fileResourceService; // Autowired
    private final RedisRepositoryImpl redisRepositoryImpl; // Autowired
    private static final String UUIDRegexPatternStr = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private static final Pattern UUIDRegexPattern = Pattern.compile(UUIDRegexPatternStr);

    @Autowired
    FileAnalyzerService(FileResourceService fileResourceService, RedisRepositoryImpl redisRepositoryImpl) {
        this.fileResourceService = fileResourceService;
        this.redisRepositoryImpl = redisRepositoryImpl;
    }

    /**
     * Find out all the files where the given fileId is referenced in, filtered by date
     *
     * @param fileId File reference to check for
     * @param date Filter out all entries that occur before this date
     * @return A list of files that refer to fileId
     */
    public List<UUID> search(String fileId, Date date) {
        final FileReferences refsFromStore = redisRepositoryImpl.getFileReferences(fileId);
        // It's possible that fileId has not been referenced in the contents of any other file
        if (null == refsFromStore)
            return Collections.emptyList();
        final Set<String> refsList = refsFromStore.getFileReferences();
        final List<String> filteredResult = refsList.stream()
                .filter(ele ->
                        redisRepositoryImpl.getFileCreationTime(ele).after(date))
                .collect(Collectors.toList());
        List<UUID> result = filteredResult.stream().map(UUID::fromString).collect(Collectors.toList());
        return result;
    }

    /**
     * Query REDIS to fetch a task by its handle
     *
     * @param taskId Task Handle (UUID as a string)
     * @return Task if found, Null otherwise
     */
    public Task findTask(final String taskId) {
        return redisRepositoryImpl.getTask(taskId);
    }

    /**
     * Task to analyze a file
     *
     * This is the task that the the Executor will execute once it takes the task from its queue. This task will perform
     * the core analysis - looking for file id references in the file being analyzed - and update the datastore with
     * the reference information.
     *
     * @param task Task information, includes the file to analyze
     */
    @Async
    public void analyze(final Task task) {
        // Put the task in RUNNING STATE
        redisRepositoryImpl.addTask(Task.updateTaskState(task, Task.TaskStatus.RUNNING));
        List<String> extractedFileIds = new ArrayList<>();
        try {
            final BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(fileResourceService.getResource(task.getFileId())));
            String line = null;
            while (null != (line = bufferedReader.readLine())) {
                logger.debug(line);
                Matcher matcher = UUIDRegexPattern.matcher(line);
                while (matcher.find()) {
                    String extractedUUID = matcher.group(0);
                    extractedFileIds.add(extractedUUID);
                    logger.debug("Extracted " + extractedUUID + " from file " + task.getFileId());
                }
            }
            redisRepositoryImpl.addTaskWithReferencesAndCreateTime(
                    new Task(task.getTaskId(), task.getCreatedAt(), task.getFileId(), Task.TaskStatus.DONE, extractedFileIds));
            logger.debug("Task {} completed", task.getTaskId());
        } catch (IOException e) {
            redisRepositoryImpl.addTask(
                    new Task(task.getTaskId(), task.getCreatedAt(), task.getFileId(), Task.TaskStatus.ERROR, new ArrayList<>()));
            logger.error("Cannot read correctly from resource " + task.getFileId());
        } catch (FileServiceException e) {
            redisRepositoryImpl.addTask(
                    new Task(task.getTaskId(), task.getCreatedAt(), task.getFileId(), Task.TaskStatus.ERROR, new ArrayList<>()));
            logger.error("Cannot open resource for reading " + task.getFileId());
        }
    }

    /**
     * Debug routine to fetch file contents from the file system
     *
     * @param resourceIdent File resource identifier
     * @return File contents by line in a Collection
     */
    public List<String> read(final String resourceIdent) {
        List<String> lines = new ArrayList<>();
        try {
            final BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(fileResourceService.getResource(resourceIdent)));
            String line = null;
            while (null != (line = bufferedReader.readLine())) {
                lines.add(line);
            }
        } catch (IOException e) {
            logger.error("cannot read correctly from resource " + resourceIdent);
            return lines;
        } catch (FileServiceException e) {
            logger.error("cannot open resource for reading" + resourceIdent);
            return lines;
        }
        return lines;
    }
}
