// File: src/main/java/com/example/batch/MemberActivityBatchApplication.java
package com.example.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MemberActivityBatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(MemberActivityBatchApplication.class, args);
    }
}

// File: src/main/java/com/example/batch/config/BatchConfig.java
package com.example.batch.config;

import com.example.batch.model.Activity;
import com.example.batch.model.Member;
import com.example.batch.processor.MemberProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.support.builder.MongoItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Bean
    public Job memberActivityJob(JobBuilderFactory jobs, Step step) {
        return jobs.get("memberActivityJob")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step memberStep(StepBuilderFactory steps,
                           MemberReader reader,
                           ItemProcessor<Member, Activity> processor,
                           MongoItemWriter<Activity> writer) {
        return steps.get("memberStep")
                .<Member, Activity>chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public MongoItemWriter<Activity> writer(MongoTemplate mongoTemplate) {
        return new MongoItemWriterBuilder<Activity>()
                .template(mongoTemplate)
                .collection("activities")
                .build();
    }

    @Bean
    @Primary
    public BatchConfigurer batchConfigurer() {
        return new DefaultBatchConfigurer() {
            @Override
            public void setDataSource(javax.sql.DataSource dataSource) {
                // Override without setting a datasource
            }
        };
    }
}

// File: src/main/java/com/example/batch/config/MemberReader.java
package com.example.batch.config;

import com.example.batch.model.Member;
import org.bson.Document;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;

@Component
public class MemberReader implements ItemReader<Member> {

    @Autowired
    private MongoTemplate mongoTemplate;

    private Iterator<Member> memberIterator;

    @Override
    public Member read() {
        if (memberIterator == null) {
            List<Member> members = mongoTemplate.aggregate(
                List.of(new Document("$match", new Document("status", "active"))),
                "members",
                Member.class
            ).getMappedResults();
            memberIterator = members.iterator();
        }
        return memberIterator.hasNext() ? memberIterator.next() : null;
    }
}

// File: src/main/java/com/example/batch/processor/MemberProcessor.java
package com.example.batch.processor;

import com.example.batch.model.Activity;
import com.example.batch.model.Member;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class MemberProcessor implements ItemProcessor<Member, Activity> {
    @Override
    public Activity process(Member member) {
        if (member.getPolicyStatus().equalsIgnoreCase("active")) {
            return new Activity(member.getId(), "Policy check", Instant.now());
        }
        return null;
    }
}

// File: src/main/java/com/example/batch/model/Member.java
package com.example.batch.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "members")
public class Member {
    @Id
    private String id;
    private String name;
    private String status;
    private String policyStatus;
}

// File: src/main/java/com/example/batch/model/Activity.java
package com.example.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@AllArgsConstructor
@Document(collection = "activities")
public class Activity {
    @Id
    private String memberId;
    private String action;
    private Instant timestamp;
}

// File: src/main/java/com/example/batch/config/SchedulerConfig.java
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

// File: src/main/java/com/example/batch/controller/BatchController.java
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

// File: src/main/resources/application.properties
spring.data.mongodb.uri=mongodb://localhost:27017/batchdb
spring.batch.job.enabled=false
