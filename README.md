# mysqlpack

MySQL pool and thread based connections wrapper on top of HikariCP and Sql2o.

It automatically hands a conn

### To use:
    public static ThreadConn con = new ThreadConn(i, url, username, password); 
* `i`: The number of connections to the database.
* `url`: The jdbc connection url for the database, as an example: `jdbc:mysql://127.0.0.1:3306/dbname`
* `username` and `password`: The user credentials for connecting to the database.

