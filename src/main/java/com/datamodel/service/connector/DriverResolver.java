package com.datamodel.service.connector;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;

public final class DriverResolver {

    private DriverResolver() {
    }

    public static String driverClass(String type) {
        if (type == null) {
            throw new BizException(ErrorCode.PARAM_ERROR, "数据源类型不能为空");
        }
        switch (type.toUpperCase()) {
            case "MYSQL":
            case "DORIS":
                return "com.mysql.cj.jdbc.Driver";
            case "DM8":
                return "dm.jdbc.driver.DmDriver";
            default:
                throw new BizException(ErrorCode.PARAM_ERROR, "不支持的数据源类型：" + type);
        }
    }
}
