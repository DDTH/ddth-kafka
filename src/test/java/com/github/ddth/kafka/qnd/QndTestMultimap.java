package com.github.ddth.kafka.qnd;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class QndTestMultimap {
    public static void main(String[] args) throws Exception {
        Multimap<String, String> mmap = HashMultimap.create();
        mmap.put("key", "value1");
        System.out.println(mmap);
        mmap.put("key", "value1");
        System.out.println(mmap);
        mmap.put("key", "value2");
        System.out.println(mmap);

        System.out.println("============================================================");
        Collection<String> value = mmap.get("key");
        System.out.println(mmap);
        System.out.println(value);

        mmap.put("key", "value3");
        System.out.println(mmap);
        System.out.println(value);

        mmap.remove("key", "value2");
        System.out.println(mmap);
        System.out.println(value);

        value.remove("value1");
        System.out.println(mmap);
        System.out.println(value);
    }
}
