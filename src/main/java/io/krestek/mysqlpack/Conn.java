package io.krestek.mysqlpack;


import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.Sql2oException;

import java.util.List;
import java.util.Map;

class Conn implements AutoCloseable {
    private Connection con;

    Conn(Sql2o sql2o) {
        con = sql2o.open();
    }

    private Query getQuery(String sql, Map<String, String> map) {
        Query query = con.createQuery(sql).throwOnMappingFailure(false);
        if (map != null)
            for (Map.Entry<String, String> entry : map.entrySet())
                if (!entry.getKey().startsWith("_"))
                    try {
                        query.addParameter(entry.getKey(), entry.getValue());
                    } catch (NullPointerException ignored) {
                    }

        return query;
    }

    <T> List<T> fetch(Class<T> type, String sql, Map<String, String> map) {
        return getQuery(sql, map).executeAndFetch(type);
    }

    void execute(String sql, Map<String, String> map) {
        try {
            getQuery(sql, map).executeUpdate();
        } catch (Sql2oException e) {
            if (e.getCause() instanceof MySQLTransactionRollbackException) {
                try {
                    Thread.sleep(200);
                    execute(sql, map);
                } catch (InterruptedException ignored) {
                }
            } else {
                throw new Sql2oException(e);
            }
        }
    }

    @Override
    public void close() {
        con.close();
        con = null;
    }
}
