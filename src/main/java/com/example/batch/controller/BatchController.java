package com.example.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
public class BatchController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job memberActivityJob;

    @GetMapping("/run")
    public String runBatchJob() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("manualRunTime", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(memberActivityJob, params);
            return "Batch job triggered successfully.";
        } catch (Exception e) {
            return "Failed to trigger batch job: " + e.getMessage();
        }
    }
}
