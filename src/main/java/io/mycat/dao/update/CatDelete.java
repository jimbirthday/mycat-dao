package io.mycat.dao.update;

import io.mycat.dao.util.NameUtil;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
@Component
public class CatDelete extends AbstractExecute {

    public String createSql(Object o, Class<?> cls) {
        //获取表名
        String tableName = NameUtil.propertyToColumn(StringUtils.uncapitalize(cls.getSimpleName()));
        String sql = " DELETE FROM ".concat(tableName);
        Map<String, Object> valueMap = createValueMap(o, cls);
        if (valueMap.size() > 0) {
            String setSqlValue = setSqlValue(valueMap);
            sql.concat(setSqlValue);
        }
        return sql;
    }

    private String setSqlValue(Map<String, Object> valueMap) {
        StringBuilder valueSql = new StringBuilder(" WHERE ");
        valueMap.forEach((k, v) -> {
            valueSql.append(k.concat(" = "));
            valueSql.append(" ? ");
        });
        return valueSql.toString();
    }

    public Map<String, Object> createValueMap(Object o, Class<?> cls) {
        Field[] fields = validateClass(cls);
        //字段集合
        Map<String, Object> values = new HashMap<>();
        for (Field field : fields) {
            field.setAccessible(true);
            String name = field.getName();
            try {
                Object o1 = field.get(o);
                if (null == o1) {
                    continue;
                }
                values.put(name, o1);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return values;
    }


    @Override
    public String createSql(Class<?> cls) {
        return null;
    }
}
