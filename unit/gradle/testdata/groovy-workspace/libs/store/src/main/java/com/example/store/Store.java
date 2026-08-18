package com.example.store;

import com.example.core.Core;

public final class Store {
    private Store() {
    }

    public static String name() {
        return "store/" + Core.name();
    }
}
