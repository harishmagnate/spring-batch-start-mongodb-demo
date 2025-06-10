package com.example.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
public class SchedulerConfig {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job memberActivityJob;

    @Scheduled(fixedRate = 900000) // 15 minutes
    public void runJob() throws Exception {
        jobLauncher.run(memberActivityJob, new org.springframework.batch.core.JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters());
    }
}
