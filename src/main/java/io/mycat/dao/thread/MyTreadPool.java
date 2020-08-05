package io.mycat.dao.thread;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author jim
 */
public class MyTreadPool {
    /**
     * 线程池核心数
     *
     */
    private static final int CORE_POOL_SIZE = 4;
    /*
     * 线程池最大数
     */
    private static final int MAXIMUM_POOL_SIZE = 20;
    /**
     * 存活时间
     */
    private static final int KEEP_ALIVE_TIME = 200;

    public static ThreadPoolExecutor threadPoolExecutor;

    /**
     * 单例构造线程池
     *
     * @return ThreadPoolExecutor
     */
    public static ThreadPoolExecutor getInstant() {
        if (null == threadPoolExecutor) {
            synchronized (MyTreadPool.class) {
                if (null == threadPoolExecutor) {
                    threadPoolExecutor = new ThreadPoolExecutor(CORE_POOL_SIZE,
                            MAXIMUM_POOL_SIZE,
                            KEEP_ALIVE_TIME,
                            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(100));
                }
            }

        }
        return threadPoolExecutor;
    }
}
