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
