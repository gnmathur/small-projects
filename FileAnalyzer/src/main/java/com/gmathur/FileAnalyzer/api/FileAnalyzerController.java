package com.gmathur.FileAnalyzer.api;

import com.gmathur.FileAnalyzer.models.Task;
import com.gmathur.FileAnalyzer.repository.RedisRepository;
import com.gmathur.FileAnalyzer.service.FileAnalyzerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;
import java.util.UUID;

@RestController
public class FileAnalyzerController {
    private static final Logger logger = LoggerFactory.getLogger(FileAnalyzerController.class);
    private static final Date epochDate = new Date(0);

    private final FileAnalyzerService fileAnalyzerService;
    private final RedisRepository redisRepository;

    @Autowired
    FileAnalyzerController(FileAnalyzerService fileAnalyzerService,
                           RedisRepository redisRepository) {
        this.fileAnalyzerService = fileAnalyzerService;
        this.redisRepository = redisRepository;
    }

    @PostMapping("/analyze/{fileId}")
    @Operation(
            summary = "Submit a file identifier for analysis",
            description = "Submitted UUID will be used a file identifier and the file will be analyzed for file " +
                    "references it might contain (other UUIDs). The API returns a handle to the async analysis job. " +
                    "A submitted job can be in either one of the following states " +
                    " * PENDING Task created and submitted to the Executor but not yet running " +
                    " * RUNNING Task being executed and analysis in progress " +
                    " * ERROR Task analysis complete with error " +
                    " * DONE Task analysis completed successfully ",
            responses = {
                    @ApiResponse( description = "Success", responseCode = "200", content = @Content(
                            mediaType = "application/json", schema = @Schema(implementation = UUID.class))),
                    @ApiResponse(description = "Bad Request", responseCode = "400", content = @Content),
                    @ApiResponse(description = "Not Found", responseCode = "404", content = @Content)
            }
    )
    public ResponseEntity<UUID> submitForAnalysis(@PathVariable (name = "fileId", required = true ) UUID fileId) {
        // Build a task and put it in PENDING state
        Task t = new Task(UUID.randomUUID().toString(), new Date(), fileId.toString(), Task.TaskStatus.PENDING);
        // Register the task in REDIS
        redisRepository.addTask(t);
        // Kick off an async analysis thread
        fileAnalyzerService.analyze(t);
        // Return the Task handle to the application
        return ResponseEntity.ok(UUID.fromString(t.getTaskId()));
    }

    @GetMapping("/analyze/{taskId}")
    @Operation(
            summary = "Find the status of a submitted task",
            description = "The API takes the task job handle returned by the job submission API and returns the Task " +
                    "details",
            responses = {
                    @ApiResponse( description = "Success", responseCode = "200", content = @Content(
                            mediaType = "application/json", schema = @Schema(implementation = Task.class))),
                    @ApiResponse(description = "Bad Request", responseCode = "400", content = @Content),
                    @ApiResponse(description = "Not Found", responseCode = "404", content = @Content)
            }
    )
    public ResponseEntity<Task> getAnalysisStatus(@PathVariable (name = "taskId", required = true ) UUID taskId) {
        final Task task = fileAnalyzerService.findTask(taskId.toString());
        if (null == task) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(task);
    }

    @GetMapping("/search")
    @Operation(
            summary = "Search for files that contain a file ID",
            description = "Search can be optionally filtered on date the files found",
            responses = {
                    @ApiResponse( description = "Success", responseCode = "200"),
                    @ApiResponse(description = "Bad Request", responseCode = "400", content = @Content),
                    @ApiResponse(description = "Not Found", responseCode = "404", content = @Content)
            }
    )
    public ResponseEntity<List<UUID>> getFileReferences(
            @RequestParam(name="fileId", required = true) UUID id,
            @RequestParam(name="from", required = false) @DateTimeFormat(pattern="yyyy-MM-dd") Date from) {
        if (null == from) {
            from = epochDate;
        }
        return ResponseEntity.ok(fileAnalyzerService.search(id.toString(), from));
    }
}
