package DBUtils;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库工具
 * Created by KIM on 2015/8/8.
 */
public class DBUtils {
    // 数据库用户名
    private final String USERNAME = "root";
    // 数据库密码
    private final String PASSWORD = "root";
    // 数据库驱动信息
    private final String DRIVER = "com.mysql.jdbc.Driver";
    // 数据库地址
    private final String URL = "jdbc:mysql://localhost:3306/";
    // 数据库名
    private final String DBNAME = "mydb";
    // 数据库连接
    private Connection connection;
    // sql语句的执行对象
    private PreparedStatement pstmt;
    // 查询返回的结果集
    private ResultSet resultSet;

    public DBUtils() {
        try {
            Class.forName(DRIVER);
            System.out.println("驱动注册成功！");
        } catch (ClassNotFoundException e) {
            System.out.println("驱动注册失败！");
            e.printStackTrace();
        }
    }

    // 获取数据库连接
    public Connection getConnection() {
        try {
            connection = DriverManager.getConnection(URL + DBNAME, USERNAME,
                    PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 完成对数据库的添加、删除、插入和修改的操作
     *
     * @param sql    sql语句（可以带占位符）
     * @param params 占位符填充元素
     * @return 数据库影响行数
     */
    public int updateByPreParedStatement(String sql, List<Object> params)
            throws SQLException {
        int result = -1;
        int index = 1;
        pstmt = connection.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (Object param : params) {
                pstmt.setObject(index++, param);
            }
        }
        result = pstmt.executeUpdate();
        return result;
    }

    /**
     * 获取单条查询结果
     *
     * @param sql    sql语句（可以带占位符）
     * @param params 占位符填充元素
     * @return 单条查询结果
     * @throws SQLException
     */
    public Map<String, Object> querySingleResult(String sql, List<Object> params)
            throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        int index = 1;
        pstmt = connection.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (Object param : params) {
                pstmt.setObject(index++, param);
            }
        }
        resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= cols_len; i++) {
                String cols_name = metaData.getColumnName(i);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_value = "";
                }
                map.put(cols_name, cols_value);
            }
        }
        return map;
    }

    /**
     * 获取多条查询结果
     *
     * @param sql    sql语句（可以带占位符）
     * @param params 占位符填充元素
     * @return 多条查询结果
     * @throws SQLException
     */
    public List<Map<String, Object>> queryResultSet(String sql,
                                                    List<Object> params) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        int index = 1;
        pstmt = connection.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (Object param : params) {
                pstmt.setObject(index++, param);
            }
        }
        resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int i = 1; i <= cols_len; i++) {
                String cols_name = metaData.getCatalogName(i);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_name = "";
                }
                map.put(cols_name, cols_value);
            }
            list.add(map);
        }
        return list;
    }

    // jdbc可以使用反射机制进行封装

    /**
     * 获取单条查询结果
     *
     * @param sql    sql语句（可以带占位符）
     * @param params 占位符填充元素
     * @param cls    对象类型
     * @return 单条查询结果
     * @throws Exception
     */
    public <T> T querySingleResult(String sql, List<Object> params, Class<T> cls)
            throws Exception {
        T resultObject = null;
        int index = 1;
        pstmt = connection.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (Object param : params) {
                pstmt.setObject(index++, param);
            }
        }
        resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            // 通过反射机制创建实例
            resultObject = cls.newInstance();
            for (int i = 1; i <= cols_len; i++) {
                String cols_name = metaData.getCatalogName(i);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_name = "";
                }
                Field field = cls.getDeclaredField(cols_name);
                field.setAccessible(true);
                field.set(resultObject, cols_value);
            }
        }
        return resultObject;
    }

    /**
     * 获取多条查询结果
     *
     * @param sql    sql语句（可以带占位符）
     * @param params 占位符填充元素
     * @param cls    对象类型
     * @return 多条查询结果
     * @throws Exception
     */
    public <T> List<T> queryResultSet(String sql, List<Object> params,
                                      Class<T> cls) throws Exception {
        List<T> list = new ArrayList<T>();
        int index = 1;
        pstmt = connection.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (Object param : params) {
                pstmt.setObject(index++, param);
            }
        }
        resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            T resultObject = cls.newInstance();
            for (int i = 1; i <= cols_len; i++) {
                String cols_name = metaData.getCatalogName(i);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_name = "";
                }
                Field field = cls.getDeclaredField(cols_name);
                field.setAccessible(true);
                field.set(resultObject, cols_value);
            }
            list.add(resultObject);
        }
        return list;
    }

    /**
     * 关闭数据库
     */
    public void close() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
