package io.mycat.dao.fetcher;

import io.mycat.dao.thread.MyThreadFactory;
import io.mycat.dao.thread.MyTreadPool;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author jim
 */
public class MyDataFetcher<T> {
    private AtomicBoolean fecthing = new AtomicBoolean(false);
    private volatile long preFecthTime;
    private volatile T cachedDate = null;
    private Supplier<T> fetcher;
    private int period = 300;

    public MyDataFetcher<T> withPeriod(int seconds) {
        if (period <= 60) {
            throw new RuntimeException("too short  period " + seconds);
        }
        this.period = seconds;
        return this;
    }

    public MyDataFetcher<T> withFetcher(Supplier<T> fetcher) {
        this.fetcher = fetcher;
        return this;
    }

    public MyDataFetcher<T> fetchNow() {
        doFetch();
        return this;
    }

    protected void doFetch() {

        if (fecthing.compareAndSet(false, true)) {
            ThreadPoolExecutor instant = MyTreadPool.getInstant();
            instant.execute(new MyThreadFactory("MyDataFetcher").newThread(() -> {
                try {
                    cachedDate = fetcher.get();
                } finally {
                    fecthing.set(false);
                }
            }));
        }

    }

    public T getCachedT() {
        if (cachedDate == null) {
            cachedDate = fetcher.get();
        } else {
            if (preFecthTime + period * 1000L < System.currentTimeMillis()) {
                doFetch();
            }
        }
        return cachedDate;

    }


}
