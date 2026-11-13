package com.datamodel.dto;

import com.datamodel.domain.DataSourceEntity;

import java.time.LocalDateTime;

/** 对外视图：永不包含密码字段。 */
public class DataSourceVO {

    private Long id;
    private String name;
    private String type;
    private String host;
    private Integer port;
    private String dbName;
    private String username;
    private String extraParams;
    private String remark;
    private String lastTestStatus;
    private LocalDateTime lastTestTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public static DataSourceVO from(DataSourceEntity entity) {
        DataSourceVO vo = new DataSourceVO();
        vo.id = entity.getId();
        vo.name = entity.getName();
        vo.type = entity.getType();
        vo.host = entity.getHost();
        vo.port = entity.getPort();
        vo.dbName = entity.getDefaultDb();
        vo.username = entity.getUsername();
        vo.extraParams = entity.getExtraParams();
        vo.remark = entity.getRemark();
        vo.lastTestStatus = toApiStatus(entity.getStatus());
        vo.lastTestTime = entity.getLastTestTime();
        vo.createTime = entity.getCreateTime();
        vo.updateTime = entity.getUpdateTime();
        return vo;
    }

    private static String toApiStatus(String status) {
        if ("OK".equalsIgnoreCase(status)) {
            return "SUCCESS";
        }
        if ("FAIL".equalsIgnoreCase(status)) {
            return "FAILED";
        }
        return null;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }

    public String getUsername() {
        return username;
    }

    public String getExtraParams() {
        return extraParams;
    }

    public String getRemark() {
        return remark;
    }

    public String getLastTestStatus() {
        return lastTestStatus;
    }

    public LocalDateTime getLastTestTime() {
        return lastTestTime;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }
}
