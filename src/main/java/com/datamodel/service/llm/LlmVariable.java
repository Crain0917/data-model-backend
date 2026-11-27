package com.datamodel.service.llm;

/** 上游字段 → Prompt 变量名映射；变量名缺省等于字段名。 */
public record LlmVariable(String field, String variable) {
}
