package com.dis.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Sequence {

    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7;

    private volatile long value;

    @SuppressWarnings("unused")
    private long q1, q2, q3, q4, q5, q6, q7;

    private static final VarHandle vh ;
    static {
        try{
            vh = MethodHandles.lookup().findVarHandle(Sequence.class,"value" ,long.class);
        }catch (Exception e){
            throw new ExceptionInInitializerError(e);
        }
    }
    public Sequence(long value) {
        this.value = value;
    }

    public long getAcquire() {
        return (long)vh.getAcquire(this);
    }
    public void setRelease(long value) {
        vh.setRelease(this,value);
    }
    public boolean compareAndSet(long expect, long update) {
        return vh.compareAndSet(this,expect,update);
    }

    public long getVolatile() {
        return value;
    }
}
