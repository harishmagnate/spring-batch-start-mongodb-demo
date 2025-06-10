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
