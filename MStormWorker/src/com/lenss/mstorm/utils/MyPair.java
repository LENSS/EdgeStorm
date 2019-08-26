package com.lenss.mstorm.utils;

import java.util.Objects;

public class MyPair<L,R> {
    public L left;
    public R right;

    public MyPair(L l, R r){
        this.left = l;
        this.right = r;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MyPair)) {
            return false;
        }
        MyPair<?, ?> p = (MyPair<?, ?>) o;
        return Objects.equals(p.left, left) && Objects.equals(p.right, right);
    }

    @Override
    public int hashCode() {
        return (left == null ? 0 : left.hashCode()) ^ (right == null ? 0 : right.hashCode());
    }

    @Override
    public String toString() {
        return "Pair{" + String.valueOf(left) + " " + String.valueOf(right) + "}";
    }

    public static <A, B> MyPair<A, B> create(A a, B b) {
        return new MyPair<A, B>(a, b);
    }
}
