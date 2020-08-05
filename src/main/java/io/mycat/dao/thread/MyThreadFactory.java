package io.mycat.dao.thread;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jim
 */
public class MyThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger nextId = new AtomicInteger(1);

    // 定义线程组名称，在问题排查时，非常有帮助
    public MyThreadFactory(String whatFeaturOfGroup) {
        namePrefix = "From MyThreadFactory's " + whatFeaturOfGroup + "-Worker-";
    }

    @Override
    public Thread newThread(Runnable task) {
        String name = namePrefix + nextId.getAndIncrement();
        return  new Thread(null, task, name, 0);
    }
}
