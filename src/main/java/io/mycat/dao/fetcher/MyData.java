package io.mycat.dao.fetcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 参数缓存类,采用mybtis一级缓存思想,直接存储在内存中
 *
 * @author jim
 */
public class MyData {

    protected static Logger log = LoggerFactory.getLogger(MyData.class);
    //缓存存放地,使用ConcurrentHashMap 防止未来有并发时,可以直接使用
    private static Map<Object, MyDataFetcher> cache = new ConcurrentHashMap<>();

    /**
     * @param t             传入对象 , 可以为任意值
     * @param myDataFetcher 核心缓存对象
     * @return
     */
    public MyData withFetcher(Function t, MyDataFetcher myDataFetcher) {
        cache.put(t.apply(t), myDataFetcher.fetchNow());
        return this;
    }

    /**
     * 效验缓存(MyDataFetcher)是否存在
     *
     * @param object
     * @return
     */
    private boolean validateObjType(Object object) {
        return null != object;
    }


    public Object getCachedT(Supplier t) {
        if (validateObjType(t.get())) {
            return cache.get(t.get()).getCachedT();
        }
        return null;
    }


}
