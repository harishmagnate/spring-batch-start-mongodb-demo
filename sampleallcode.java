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

// File: src/main/java/com/example/batch/config/BatchInfrastructureConfig.java
package com.example.batch.config;

import org.springframework.batch.core.JobRepository;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.ResourcelessTransactionManager;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncherBuilder;
import org.springframework.core.task.SyncTaskExecutor;


import java.util.Objects;

@Configuration
public class BatchInfrastructureConfig {

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobRepository jobRepository(MongoTemplate mongoTemplate, PlatformTransactionManager transactionManager) {
        return new org.springframework.batch.extensions.mongo.MongoJobRepositoryFactoryBean()
                .mongoTemplate(mongoTemplate)
                .transactionManager(transactionManager)
                .getObject();
    }

    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Bean
public JobLauncher jobLauncher(JobRepository jobRepository) {
    return new TaskExecutorJobLauncherBuilder()
            .jobRepository(jobRepository)
            .taskExecutor(new SyncTaskExecutor()) // or use a ThreadPoolTaskExecutor
            .build();
}
}

// File: src/main/java/com/example/batch/config/BatchConfig.java
package com.example.batch.config;

import com.example.batch.model.Activity;
import com.example.batch.model.Member;
import com.example.batch.processor.MemberProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.support.builder.MongoItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

    @Bean
    public Job memberActivityJob(JobRepository jobRepository, Step memberStep) {
        return new JobBuilder("memberActivityJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(memberStep)
                .build();
    }

    @Bean
    public Step memberStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager,
                           MemberReader reader,
                           ItemProcessor<Member, Activity> processor,
                           MongoItemWriter<Activity> writer) {
        return new StepBuilder("memberStep", jobRepository)
                .<Member, Activity>chunk(10, transactionManager)
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
}

// File: src/main/resources/application.properties
spring.data.mongodb.uri=mongodb://localhost:27017/batchdb
spring.batch.job.enabled=false
spring.batch.jdbc.enabled=false
