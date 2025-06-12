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
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MongoJobRepositoryFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.ResourcelessTransactionManager;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncherBuilder;
import org.springframework.core.task.SyncTaskExecutor;

@Configuration
public class BatchInfrastructureConfig {

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobRepository jobRepository(MongoTemplate mongoTemplate, PlatformTransactionManager transactionManager) throws Exception {
        MongoJobRepositoryFactoryBean factoryBean = new MongoJobRepositoryFactoryBean();
        factoryBean.setMongoTemplate(mongoTemplate);
        factoryBean.setTransactionManager(transactionManager);
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }

    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        return new TaskExecutorJobLauncherBuilder()
                .jobRepository(jobRepository)
                .taskExecutor(new SyncTaskExecutor())
                .build();
    }
}


// File: src/main/java/com/example/batch/config/BatchConfig.java
package com.example.batch.config;

import com.example.batch.model.Member;
import com.example.batch.model.MemberAudit;
import com.example.batch.processor.MemberProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.support.builder.MongoItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

    @Bean
    public Job memberAuditJob(JobRepository jobRepository, Step memberAuditStep) {
        return new JobBuilder("memberAuditJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(memberAuditStep)
                .build();
    }

    @Bean
    public Step memberAuditStep(JobRepository jobRepository,
                                 PlatformTransactionManager transactionManager,
                                 MemberReader reader,
                                 MemberProcessor processor,
                                 MongoItemWriter<MemberAudit> writer) {
        return new StepBuilder("memberAuditStep", jobRepository)
                .<Member, MemberAudit>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public MongoItemWriter<MemberAudit> writer(MongoTemplate mongoTemplate) {
        return new MongoItemWriterBuilder<MemberAudit>()
                .template(mongoTemplate)
                .collection("memberaudit")
                .build();
    }
}

// File: src/main/java/com/example/batch/reader/MemberReader.java
package com.example.batch.reader;

import com.example.batch.model.Member;
import org.bson.Document;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class MemberReader extends MongoItemReader<Member> {

    public MemberReader(MongoTemplate mongoTemplate) {
        super.setTemplate(mongoTemplate);
        super.setTargetType(Member.class);
        super.setQuery("{}"); // Match all

        Map<String, Sort.Direction> sort = new HashMap<>();
        sort.put("_id", Sort.Direction.ASC);
        super.setSort(sort);
        super.setPageSize(100);
        super.setCollection("members");
    }
}

// File: src/main/java/com/example/batch/processor/MemberProcessor.java
package com.example.batch.processor;

import com.example.batch.model.Member;
import com.example.batch.model.MemberAudit;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class MemberProcessor implements ItemProcessor<Member, MemberAudit> {
    @Override
    public MemberAudit process(Member member) {
        if ("active".equalsIgnoreCase(member.getStatus())) {
            return new MemberAudit(member.getId(), member.getName(), member.getStatus());
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
}

// File: src/main/java/com/example/batch/model/MemberAudit.java
package com.example.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@AllArgsConstructor
@Document(collection = "memberaudit")
public class MemberAudit {
    @Id
    private String memberId;
    private String name;
    private String status;
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
    private Job memberAuditJob;

    @Scheduled(fixedRate = 900000) // 15 minutes
    public void runJob() throws Exception {
        jobLauncher.run(memberAuditJob, new org.springframework.batch.core.JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters());
    }
}

// File: src/main/resources/application.properties
spring.data.mongodb.uri=mongodb://localhost:27017/batchdb
spring.batch.job.enabled=false
spring.batch.jdbc.enabled=false
