package io.krestek.mysqlpack;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sql2o.Sql2o;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings({"WeakerAccess", "unused"})
public class ThreadConn {
    Sql2o sql2o;

    public ThreadConn(int poolSize, String url, String username, String password) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setMaximumPoolSize(poolSize);
        DataSource dataSource = new HikariDataSource(hikariConfig);
        sql2o = new Sql2o(dataSource);
    }


    private ThreadLocal<Conn> localCon = new ThreadLocal<>();


    private Conn getCon() {
        if (localCon.get() == null)
            localCon.set(new Conn(sql2o));
        return localCon.get();
    }

    public void close() {
        Conn con = localCon.get();
        if (con != null)
            con.close();
        localCon.remove();
        token.remove();
    }

    public void closeConnectionOnly() {
        Conn con = localCon.get();
        if (con != null)
            con.close();
        localCon.remove();
    }


    public <T> T fetchOne(Class<T> type, String sql, HashMap<String, String> map) {
        return getCon().fetchOne(type, sql, map, getToken());
    }

    public <T> List<T> fetch(Class<T> type, String sql, HashMap<String, String> map) {
        return getCon().fetch(type, sql, map, getToken());
    }

    public void execute(String sql, HashMap<String, String> map) {
        getCon().execute(sql, map, getToken());
    }

    //Extra:
    private ThreadLocal<String> token = new ThreadLocal<>();

    public String getToken() {
        return token.get();
    }

    public void setToken(String token) {
        if (this.token.get() != null)
            System.out.println("ERROR: Token already set!");
        else
            this.token.set(token);
    }
}
