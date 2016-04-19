package com.liamsharp;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    
    
    final private static String DB_DIR = 
            System.getProperty("user.home")
            + File.separator 
            + "derby_db_test";
    
    final private static String DB = 
            DB_DIR
            + File.separator
            + "db";

    public static void main( String[] args )
    {
        
        loadApacheDerbyDriver();
        int loop = 1;
        try
        {
            final File dir = new File(DB_DIR);
            LOGGER.info( "DB_DIR:" + DB_DIR );

            if (dir.exists())
            {
                FileUtils.deleteDirectory(dir);
            }
            final boolean dirMake = dir.mkdir();
            LOGGER.info( "dirMake:" + dirMake );
        
            while (true)
            {
                LOGGER.info("Loop: " + loop);
                openAndClose(dir);
                ++loop;
            }
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed after loop " + loop + ", e: " + e, e);
        }
    }

    private static void openAndClose(
        final File dir) throws Exception
    {
        createConnection(DB);
        shutdown(DB);
        deleteDirectoryWithBackOff(dir, 5);
    }
    
    private static void deleteDirectoryWithBackOff(
        final File dir,
        int maxTries)
    {
        long waitTimeMillis = 100;
        
        for (int i = 1; i <= maxTries; i++)
        {
            try
            {
                FileUtils.deleteDirectory(dir);
                break;
            }
            catch (IOException e)
            {
                LOGGER.warn("Failed to delete directory on try " + i + " of " + maxTries 
                        + ". Will wait " + waitTimeMillis + "ms and try again, dir:" + dir + ", e: " + e);
                sleep(waitTimeMillis);
                waitTimeMillis *= 2;
            }
        }
    }
    
    private static void sleep(
        final long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            LOGGER.warn("Interupted whilst sleeping " + e);
        }
    }
    
    private static void loadApacheDerbyDriver()
    {
        final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        try
        {
            Class.forName(driver);
        }
        catch (ClassNotFoundException e)
        {
            LOGGER.fatal("Unable to load database driver " + driver + ", " + e, e);
            System.exit(1);
        }
    }
    
    public static Connection createConnection(
        final String dirPath) throws SQLException
    {
        final String javaDbConnectionUri = getConnectionUri(dirPath) + ";create=true"; 
        
        final Connection conn = DriverManager.getConnection(javaDbConnectionUri);

        LOGGER.info("Creating JavaDB Database " + javaDbConnectionUri);
        return conn;
    }

    public static void shutdown(
        String dirPath)
    {
        final String javaDbConnectionUri = getConnectionUri(dirPath) + ";shutdown=true"; 
        LOGGER.info("Shutting down database, location: " + dirPath + ", uri: " + javaDbConnectionUri);
        try
        {
            DriverManager.getConnection(javaDbConnectionUri);
            LOGGER.warn("Failed to shutdown database, location: " + dirPath 
                    + ", uri: " + javaDbConnectionUri);
        }
        catch (SQLException e)
        {
            LOGGER.info("Shutdown database, location: " + dirPath 
                    + ", uri: " + javaDbConnectionUri);
            // Database shutdown as expected, strangely it throws exceptions to indicate success.
            // https://db.apache.org/derby/docs/10.9/devguide/tdevdvlp40464.html
        }
    }

    private static String getConnectionUri(
        final String dirPath)
    {
        return "jdbc:derby:" + dirPath; 
    }

}
