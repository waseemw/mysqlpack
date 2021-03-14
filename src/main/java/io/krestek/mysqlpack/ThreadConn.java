package io.krestek.mysqlpack;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sql2o.Sql2o;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"WeakerAccess", "unused"})
public class ThreadConn {
    private final Sql2o sql2o;
    private boolean connectionPerThread;
    private ThreadLocal<Map<String, Object>> threadParams;
    private ThreadLocal<Conn> localCon;

    public ThreadConn(int poolSize, String url, String username, String password) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(poolSize);
        DataSource dataSource = new HikariDataSource(hikariConfig);
        sql2o = new Sql2o(dataSource);
    }

    public ThreadConn(int poolSize, String url, String username, String password, boolean connectionPerThread) {
        this.connectionPerThread = connectionPerThread;
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(poolSize);
        DataSource dataSource = new HikariDataSource(hikariConfig);
        sql2o = new Sql2o(dataSource);
    }


    public void close() {
        if (localCon == null)
            throw new RuntimeException("this method should only be called if connectionPerThread is set to true.");
        Conn con = localCon.get();
        if (con != null)
            con.close();
        localCon.remove();
        clear();
    }

    public void closeConnectionOnly() {
        if (localCon == null)
            throw new RuntimeException("this method should only be called if connectionPerThread is set to true.");
        Conn con = localCon.get();
        if (con != null)
            con.close();
        localCon.remove();
    }

    public void clear() {
        if (threadParams != null)
            threadParams.remove();
    }


    public <T> T fetchOne(Class<T> type, String sql, Map<String, Object> map) {
        return fetch(type, sql, map).get(0);
    }

    public <T> List<T> fetch(Class<T> type, String sql, Map<String, Object> map) {
        if (connectionPerThread)
            return getCon().fetch(type, sql, mergeMaps(map));

        try (Conn con = new Conn(sql2o)) {
            return con.fetch(type, sql, mergeMaps(map));
        }
    }

    public void execute(String sql, Map<String, Object> map) {
        if (connectionPerThread) {
            getCon().execute(sql, mergeMaps(map));
            return;
        }

        try (Conn con = new Conn(sql2o)) {
            con.execute(sql, mergeMaps(map));
        }
    }


    public Object getThreadParam(String key) {
        return threadParams.get().get(key);
    }

    public void putThreadParam(String key, String value) {
        if (threadParams == null)
            threadParams = new ThreadLocal<>();
        if (threadParams.get() == null)
            threadParams.set(new HashMap<>());
        threadParams.get().put(key, value);
    }


    private Conn getCon() {
        if (localCon == null)
            localCon = new ThreadLocal<>();
        if (localCon.get() == null)
            localCon.set(new Conn(sql2o));
        return localCon.get();
    }

    private Map<String, Object> mergeMaps(Map<String, Object> map) {
        if (threadParams == null)
            return map;

        Map<String, Object> threadMap = threadParams.get();

        if (map == null)
            return threadMap;

        threadMap.forEach(map::putIfAbsent);
        return map;
    }
}
