package com.datamodel.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowDeleteConfirmTest {

    @Test
    void exactNameMatches() {
        assertTrue(FlowService.confirmNameMatches("订单同步任务", "订单同步任务"));
    }

    @Test
    void trimsUserInput() {
        assertTrue(FlowService.confirmNameMatches("order-sync", "  order-sync  "));
    }

    @Test
    void isCaseSensitive() {
        assertFalse(FlowService.confirmNameMatches("Order-Sync", "order-sync"));
    }

    @Test
    void missingOrEmptyNeverMatches() {
        assertFalse(FlowService.confirmNameMatches("order-sync", null));
        assertFalse(FlowService.confirmNameMatches("order-sync", ""));
        assertFalse(FlowService.confirmNameMatches("order-sync", "   "));
        assertFalse(FlowService.confirmNameMatches(null, "order-sync"));
    }
}
