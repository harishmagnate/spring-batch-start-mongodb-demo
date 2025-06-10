package com.example.batch.config;

import com.example.batch.model.Activity;
import com.example.batch.model.Member;
import com.example.batch.processor.MemberProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.support.builder.MongoItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
}
