package com.datamodel.service.llm;

import com.datamodel.common.JsonUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按渲染后的 Prompt 字符数控制单次请求规模：
 * 批量模式超过阈值时按行对半拆批；逐行模式每行一批；单行仍超限由调用方降级为 unknownValue。
 */
public final class LlmBatchPlanner {

    private final int maxInputChars;

    public LlmBatchPlanner(int maxInputChars) {
        this.maxInputChars = Math.max(1000, maxInputChars);
    }

    public Plan plan(List<LlmRow> rows, LlmConfig config) {
        Plan plan = new Plan();
        if (config.rowMode()) {
            for (LlmRow row : rows) {
                String prompt = rowPrompt(row, config);
                if (prompt.length() > maxInputChars) {
                    plan.overlong.add(row);
                } else {
                    plan.batches.add(new Batch(List.of(row), inputText(List.of(row), config),
                            prompt));
                }
            }
            return plan;
        }
        split(new ArrayList<>(rows), config, plan);
        return plan;
    }

    private void split(List<LlmRow> rows, LlmConfig config, Plan plan) {
        if (rows.isEmpty()) {
            return;
        }
        String inputText = inputText(rows, config);
        String prompt = config.renderPrompt(inputText, null);
        if (prompt.length() <= maxInputChars) {
            plan.batches.add(new Batch(rows, inputText, prompt));
            return;
        }
        if (rows.size() == 1) {
            plan.overlong.add(rows.get(0));
            return;
        }
        int mid = rows.size() / 2;
        split(new ArrayList<>(rows.subList(0, mid)), config, plan);
        split(new ArrayList<>(rows.subList(mid, rows.size())), config, plan);
    }

    /**
     * 逐行模式的 Prompt：模板按该行变量渲染，末尾追加该行命名 payload 与输出格式说明。
     * 模板本身不含 id，没有这段说明模型无法在 [{"id","value"}] 中回填记录 id。
     */
    static String rowPrompt(LlmRow row, LlmConfig config) {
        return config.renderPrompt("", row)
                + "\n\n当前记录（JSON 数组，仅含该行）：\n" + inputText(List.of(row), config)
                + "\n请只输出 JSON 数组，格式为 [{\"id\":\"" + row.idText()
                + "\",\"value\":\"枚举值\"}]，禁止代码块标记与解释。";
    }

    /** 请求体中的 input_text：JSON 数组 [{"id":"1","<变量>":"值"}]，id 一律字符串。 */
    public static String inputText(List<LlmRow> rows, LlmConfig config) {
        List<Map<String, Object>> items = new ArrayList<>(rows.size());
        for (LlmRow row : rows) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", row.idText());
            for (LlmVariable variable : config.variables()) {
                item.put(variable.variable(), row.value(variable.field()));
            }
            items.add(item);
        }
        return JsonUtils.toJson(items);
    }

    public static final class Batch {
        private final List<LlmRow> rows;
        private final String inputText;
        private final String prompt;

        Batch(List<LlmRow> rows, String inputText, String prompt) {
            this.rows = Collections.unmodifiableList(rows);
            this.inputText = inputText;
            this.prompt = prompt;
        }

        public List<LlmRow> rows() {
            return rows;
        }

        public String inputText() {
            return inputText;
        }

        public String prompt() {
            return prompt;
        }

        public long minId() {
            return rows.isEmpty() ? 0 : rows.get(0).id();
        }

        public long maxId() {
            return rows.isEmpty() ? 0 : rows.get(rows.size() - 1).id();
        }
    }

    public static final class Plan {
        private final List<Batch> batches = new ArrayList<>();
        private final List<LlmRow> overlong = new ArrayList<>();

        public List<Batch> batches() {
            return batches;
        }

        public List<LlmRow> overlong() {
            return overlong;
        }
    }
}
