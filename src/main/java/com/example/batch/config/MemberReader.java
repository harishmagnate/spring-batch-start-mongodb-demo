package com.example.batch.config;

import com.example.batch.model.Member;
import org.bson.Document;
import org.springframework.batch.item.ExecutionContext;
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
