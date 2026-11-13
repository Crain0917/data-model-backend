package com.datamodel.dto;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

public class DataSourceRequest {

    @NotBlank(message = "数据源名称不能为空")
    @Size(max = 128, message = "数据源名称不能超过 128 字符")
    private String name;

    @NotBlank(message = "数据源类型不能为空")
    @Pattern(regexp = "(?i)MYSQL|DORIS|DM8", message = "数据源类型仅支持 MYSQL / DORIS / DM8")
    private String type;

    @NotBlank(message = "主机地址不能为空")
    @Size(max = 128, message = "主机地址不能超过 128 字符")
    @Pattern(regexp = "[A-Za-z0-9._:-]+", message = "主机地址格式不合法")
    private String host;

    @NotNull(message = "端口不能为空")
    @Min(value = 1, message = "端口范围 1-65535")
    @Max(value = 65535, message = "端口范围 1-65535")
    private Integer port;

    @NotBlank(message = "默认库不能为空")
    @Size(max = 128, message = "默认库不能超过 128 字符")
    @Pattern(regexp = "[A-Za-z0-9_$-]+", message = "默认库名称格式不合法")
    private String dbName;

    @NotBlank(message = "用户名不能为空")
    @Size(max = 128, message = "用户名不能超过 128 字符")
    private String username;

    /** 仅写入；更新时为空表示不修改。接口任何响应都不返回此字段。 */
    @Size(max = 512, message = "密码不能超过 512 字符")
    private String password;

    @Size(max = 2000, message = "额外参数不能超过 2000 字符")
    private String extraParams;

    @Size(max = 512, message = "备注不能超过 512 字符")
    private String remark;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getExtraParams() {
        return extraParams;
    }

    public void setExtraParams(String extraParams) {
        this.extraParams = extraParams;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
