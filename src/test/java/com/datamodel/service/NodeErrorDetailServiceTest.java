package com.datamodel.service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeErrorDetailServiceTest {

    @Test
    void extractsDorisErrorUrlFromMessage() {
        String message = "Stream Load FAILED：bad row，错误详情："
                + "http://192.168.10.13:8040/api/_load_error_log?file=__shard_0/err&token=a&amp;b";
        String url = NodeErrorDetailService.extractErrorUrl(message);
        assertEquals("http://192.168.10.13:8040/api/_load_error_log?file=__shard_0/err&token=a&b", url);

        assertNull(NodeErrorDetailService.extractErrorUrl(null));
        assertNull(NodeErrorDetailService.extractErrorUrl("普通错误，没有链接"));
    }

    @Test
    void htmlIsStrippedAndDecoded() {
        String html = "<html><head><style>p{color:red}</style></head><body>"
                + "<h1>Error</h1><p>bad&amp;row &lt;2&gt;</p><script>x=1</script></body></html>";
        String text = NodeErrorDetailService.htmlToText(html);
        assertTrue(text.contains("Error"), text);
        assertTrue(text.contains("bad&row <2>"), text);
        assertTrue(!text.contains("color:red"), text);
        assertTrue(!text.contains("x=1"), text);
    }

    @Test
    void jsonMessageIsPreferred() {
        String json = "{\"Status\":\"Fail\",\"message\":\"column count mismatch\"}";
        NodeErrorDetailService.Normalized normalized = NodeErrorDetailService.normalize(
                json.getBytes(StandardCharsets.UTF_8), "application/json");
        assertEquals("column count mismatch", normalized.text);
    }

    @Test
    void longLogIsTruncatedAtSixtyFourKb() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 70000; i++) {
            sb.append('x');
        }
        NodeErrorDetailService.Normalized normalized = NodeErrorDetailService.normalize(
                sb.toString().getBytes(StandardCharsets.UTF_8), "text/plain");
        assertEquals(64 * 1024, normalized.text.length());
        assertTrue(normalized.truncated);
    }
}
