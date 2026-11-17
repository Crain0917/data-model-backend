package com.datamodel.dto;

import com.datamodel.domain.RunLogEntity;

import java.time.LocalDateTime;

public class RunLogVO {

    private LocalDateTime time;
    private String level;
    private String message;

    public static RunLogVO from(RunLogEntity e) {
        RunLogVO vo = new RunLogVO();
        vo.time = e.getCreateTime();
        vo.level = e.getLevel();
        vo.message = e.getMessage();
        return vo;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }
}
