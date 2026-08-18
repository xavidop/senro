package com.example.datastore;

import com.example.core.Core;

public final class DataStore {
    private DataStore() {
    }

    public static String name() {
        return "data-store/" + Core.name();
    }
}
