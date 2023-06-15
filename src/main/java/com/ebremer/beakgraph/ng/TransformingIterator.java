package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import java.util.function.Function;

public class TransformingIterator<T, R> implements Iterator<R> {
    private final Iterator<T> source;
    private final Function<T, R> transformation;

    public TransformingIterator(Iterator<T> source, Function<T, R> transformation) {
        this.source = source;
        this.transformation = transformation;
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public R next() {
        return transformation.apply(source.next());
    }
}
