package com.github.ddth.kafka.qnd;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QndConcurrentMap {

    static String compute(String input) {
        System.out.println("Input: " + input);
        return input;
    }

    public static void main(String[] args) {
        ConcurrentMap<String, Object> map = new ConcurrentHashMap<>();

        map.computeIfAbsent("haha", s -> compute("1"));
        System.out.println(map.get("haha"));

        map.computeIfAbsent("haha", s -> compute("2"));
        System.out.println(map.get("haha"));
    }
}
