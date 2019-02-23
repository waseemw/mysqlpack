package io.krestek.mysqlpack;


import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.Sql2oException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Conn {
    private Connection con;

    Conn(Sql2o sql2o) {
        con = sql2o.open();
    }

    private Query getQuery(String sql, HashMap<String, String> map, String token) {
        Query query = con.createQuery(sql).throwOnMappingFailure(false);
        if (map != null)
            for (Map.Entry<String, String> entry : map.entrySet())
                if (!entry.getKey().startsWith("_"))
                    try {
                        query.addParameter(entry.getKey(), entry.getValue());
                    } catch (NullPointerException ignored) {
                    }
        try {
            if (token != null && (map == null || !map.containsKey("token")))
                query.addParameter("token", token);
        } catch (NullPointerException ignored) {
        }

        return query;
    }

    <T> T fetchOne(Class<T> type, String sql, HashMap<String, String> map, String token) {
        return getQuery(sql, map, token).executeAndFetch(type).get(0);
    }

    <T> List<T> fetch(Class<T> type, String sql, HashMap<String, String> map, String token) {
        return getQuery(sql, map, token).executeAndFetch(type);
    }

    void execute(String sql, HashMap<String, String> map, String token) {
        try {
            getQuery(sql, map, token).executeUpdate();
        } catch (Sql2oException e) {
            if (e.getCause() instanceof MySQLTransactionRollbackException) {
                try {
                    Thread.sleep(200);
                    execute(sql, map, token);
                } catch (InterruptedException ignored) {
                }
            } else {
                throw new Sql2oException(e);
            }
        }
    }

    void close() {
        con.close();
        con = null;
    }
}