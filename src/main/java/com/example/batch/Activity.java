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
