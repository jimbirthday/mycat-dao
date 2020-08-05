package io.mycat.dao.update;

import java.lang.reflect.Field;
import java.util.Map;

public abstract class AbstractExecute {

    public abstract String createSql(Class<?> cls);

    public Field[] validateClass(Class<?> cls) {
        Field[] fields = cls.getDeclaredFields();
        if (fields.length == 0) {
            throw new RuntimeException("the  entity fields is empty");
        }
        return fields;
    }
}
