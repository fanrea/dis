package com.dis.benchmark;

// JMH 启动入口。
public final class JmhBenchmarkMain {
    private JmhBenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
