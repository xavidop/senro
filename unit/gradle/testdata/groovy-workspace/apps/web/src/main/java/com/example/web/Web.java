package com.example.web;

import com.example.store.Store;

public final class Web {
    private Web() {
    }

    public static String greeting() {
        return "web/" + Store.name();
    }
}
