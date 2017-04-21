package ec.ecust.edu.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Created by pengfei on 2017/4/21.
 */
public abstract  class MysqlBase {
    private final static Logger log = LoggerFactory.getLogger(MysqlBase.class) ;
    protected final static Properties p = new Properties() ;
    private final static String tmpPath = "tmp.txt" ;
    protected File file = new File(tmpPath) ;
    static{
        InputStream in = MysqlClusterSolver.class.getClassLoader()
                .getResourceAsStream("jdbc.properties") ;
        try {
            p.load(in) ;
            log.info("===========加载jdbc properties驱动:==============") ;
            Enumeration enumeration = p.propertyNames() ;
            while(enumeration.hasMoreElements()){
                String key = (String) enumeration.nextElement();
                log.info(key + "=" + p.get(key)) ;
            }
            log.info("================================================") ;
            Class.forName(p.getProperty("driverClass")) ;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() throws SQLException {
         return  DriverManager.getConnection(p.getProperty("url")
                ,p.getProperty("username"), (String) p.get("password")) ;
    }

}
