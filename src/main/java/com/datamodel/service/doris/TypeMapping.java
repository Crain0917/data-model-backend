package com.datamodel.service.doris;

/** 单个源类型的映射结果。 */
public class TypeMapping {

    private final String dorisType;
    private final String warning;
    private final boolean fallback;

    public TypeMapping(String dorisType, String warning, boolean fallback) {
        this.dorisType = dorisType;
        this.warning = warning;
        this.fallback = fallback;
    }

    public static TypeMapping of(String dorisType) {
        return new TypeMapping(dorisType, null, false);
    }

    public static TypeMapping of(String dorisType, String warning) {
        return new TypeMapping(dorisType, warning, false);
    }

    public static TypeMapping fallback(String dorisType, String warning) {
        return new TypeMapping(dorisType, warning, true);
    }

    public String getDorisType() {
        return dorisType;
    }

    public String getWarning() {
        return warning;
    }

    public boolean isFallback() {
        return fallback;
    }
}
