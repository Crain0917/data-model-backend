package com.datamodel.service.connector;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;

/**
 * 将 JDBC 异常归类为 API 契约 §2.5 的六类 errorType。
 */
public final class ConnectionErrorClassifier {

    private ConnectionErrorClassifier() {
    }

    public static String classify(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ClassNotFoundException) {
                return "DRIVER_MISSING";
            }
            if (current instanceof SocketTimeoutException) {
                return "TIMEOUT";
            }
            if (current instanceof ConnectException) {
                return "REFUSED";
            }
            if (current instanceof SQLException) {
                SQLException sql = (SQLException) current;
                int code = sql.getErrorCode();
                String state = sql.getSQLState();
                String msg = lower(sql.getMessage());
                if (code == 1045 || "28000".equals(state)
                        || (msg != null && (msg.contains("access denied")
                        || msg.contains("invalid username") || msg.contains("invalid password")
                        || msg.contains("wrong password") || msg.contains("authentication")))) {
                    return "AUTH_FAIL";
                }
                if (code == 1049 || (msg != null && (msg.contains("unknown database")
                        || msg.contains("database does not exist")))) {
                    return "DB_NOT_FOUND";
                }
                if (msg != null && (msg.contains("no suitable driver")
                        || msg.contains("driver class not found"))) {
                    return "DRIVER_MISSING";
                }
                if (msg != null && (msg.contains("timed out") || msg.contains("timeout"))) {
                    return "TIMEOUT";
                }
                if (msg != null && (msg.contains("connection refused") || msg.contains("connection timed out"))) {
                    return "REFUSED";
                }
            }
            String msg = lower(current.getMessage());
            if (msg != null) {
                if (msg.contains("no suitable driver") || msg.contains("driver class not found")) {
                    return "DRIVER_MISSING";
                }
                if (msg.contains("timed out") || msg.contains("timeout")) {
                    return "TIMEOUT";
                }
                if (msg.contains("connection refused") || msg.contains("10061")) {
                    return "REFUSED";
                }
            }
            current = current.getCause();
        }
        return "OTHER";
    }

    public static String messageOf(String errorType, Throwable throwable) {
        switch (errorType) {
            case "REFUSED":
                return "无法连接到服务器，请检查主机与端口";
            case "AUTH_FAIL":
                return "用户名或密码错误";
            case "DB_NOT_FOUND":
                return "默认库不存在，请检查库名";
            case "TIMEOUT":
                return "连接超时，请检查网络与参数";
            case "DRIVER_MISSING":
                return "该类型驱动未安装";
            default:
                String raw = throwable == null ? "" : throwable.getMessage();
                return "连接失败：" + (raw == null ? "未知错误" : raw);
        }
    }

    private static String lower(String value) {
        return value == null ? null : value.toLowerCase();
    }
}
