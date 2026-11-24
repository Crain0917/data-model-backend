package com.datamodel.service;

import com.datamodel.common.BizException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlowValidationNodeTypeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode dag(String json) throws Exception {
        return mapper.readTree(json);
    }

    private static String source() {
        return "{\"id\":\"s1\",\"type\":\"source\",\"data\":{\"config\":{"
                + "\"dataSourceId\":1,\"tableName\":\"t\",\"fields\":["
                + "{\"name\":\"a\",\"type\":\"DOUBLE\"}]}}}";
    }

    private static String edge(String source, String target) {
        return "{\"id\":\"e_" + source + "_" + target + "\",\"source\":\"" + source
                + "\",\"target\":\"" + target
                + "\",\"sourceHandle\":\"out\",\"targetHandle\":\"in-0\"}";
    }

    @Test
    void calculatorAndFormulaAreKnownSingleInputTypes() throws Exception {
        String nodes = source() + ","
                + "{\"id\":\"k1\",\"type\":\"calculator\",\"data\":{\"config\":{"
                + "\"calculations\":[{\"output\":\"x\",\"operation\":\"ADD\","
                + "\"fieldA\":\"a\",\"fieldB\":\"a\"}]}}},"
                + "{\"id\":\"f1\",\"type\":\"formula\",\"data\":{\"config\":{"
                + "\"formulas\":[{\"output\":\"y\",\"expression\":\"[x]+[a]\"}]}}}";
        JsonNode json = dag("{\"nodes\":[" + nodes + "],\"edges\":["
                + edge("s1", "k1") + "," + edge("k1", "f1") + "]}");

        assertDoesNotThrow(() -> FlowValidation.validate(json));
    }

    @Test
    void newOperatorTypesAreKnownSingleInputTypes() throws Exception {
        String nodes = source() + ","
                + "{\"id\":\"r1\",\"type\":\"replace_null\",\"data\":{\"config\":{"
                + "\"rules\":[{\"field\":\"a\",\"strategy\":\"MAX\"}]}}},"
                + "{\"id\":\"o1\",\"type\":\"sort_rows\",\"data\":{\"config\":{}}},"
                + "{\"id\":\"p1\",\"type\":\"split_fields\",\"data\":{\"config\":{}}},"
                + "{\"id\":\"x1\",\"type\":\"split_fields_to_rows\",\"data\":{\"config\":{}}}";
        JsonNode json = dag("{\"nodes\":[" + nodes + "],\"edges\":["
                + edge("s1", "r1") + "," + edge("r1", "o1") + ","
                + edge("o1", "p1") + "," + edge("p1", "x1") + "]}");

        assertDoesNotThrow(() -> FlowValidation.validate(json));
    }

    @Test
    void formulaWithoutUpstreamIsRejected() throws Exception {
        JsonNode json = dag("{\"nodes\":[{\"id\":\"f1\",\"type\":\"formula\","
                + "\"data\":{\"config\":{\"formulas\":[]}}}],\"edges\":[]}");
        BizException e = assertThrows(BizException.class, () -> FlowValidation.validate(json));
        assertEquals("IN_DEGREE", ((Map<?, ?>) e.getData()).get("reason"));
    }

    @Test
    void unknownTypeIsStillRejected() throws Exception {
        JsonNode json = dag("{\"nodes\":[{\"id\":\"x1\",\"type\":\"calculator2\","
                + "\"data\":{\"config\":{}}}],\"edges\":[]}");
        BizException e = assertThrows(BizException.class, () -> FlowValidation.validate(json));
        assertEquals("NODE_TYPE_UNKNOWN", ((Map<?, ?>) e.getData()).get("reason"));
    }
}
