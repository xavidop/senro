package com.example.web;

import com.example.datastore.DataStore;

public final class Web {
    private Web() {
    }

    public static String greeting() {
        return "web/" + DataStore.name();
    }
}
