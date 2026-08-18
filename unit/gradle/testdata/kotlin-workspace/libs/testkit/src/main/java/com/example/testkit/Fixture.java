package com.example.testkit;

import com.example.core.Core;

public final class Fixture {
    private Fixture() {
    }

    public static String sample() {
        return "fixture/" + Core.name();
    }
}
