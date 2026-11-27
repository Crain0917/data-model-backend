package com.datamodel.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Key 状态：只回 hasKey + mask，绝不下发明文或密文。 */
@JsonInclude(JsonInclude.Include.ALWAYS)
public class LlmKeyStatus {

    private boolean hasKey;
    private String mask;

    public LlmKeyStatus() {
    }

    public LlmKeyStatus(boolean hasKey, String mask) {
        this.hasKey = hasKey;
        this.mask = mask;
    }

    public boolean isHasKey() {
        return hasKey;
    }

    public void setHasKey(boolean hasKey) {
        this.hasKey = hasKey;
    }

    public String getMask() {
        return mask;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }
}
