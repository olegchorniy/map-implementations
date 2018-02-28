package com.test.map;

public interface SimpleMap<K, V> {

    V get(K key);

    V put(K key, V value);

    V remove(K key);
}
