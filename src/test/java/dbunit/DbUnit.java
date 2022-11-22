package dbunit;

import org.dbunit.DBTestCase;
import org.dbunit.PropertiesBasedJdbcDatabaseTester;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;

import java.io.FileInputStream;
import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DbUnit extends DBTestCase {

    private Connection conn = null;
    private Statement stmt = null;
    public DbUnit(String name) throws SQLException {
        super( name );
        System.setProperty( PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, "org.postgresql.Driver" );
        System.setProperty( PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL, "jdbc:postgresql://localhost:5432/khoi_tracker_test" );
        System.setProperty( PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, "hnkhoi" );
        System.setProperty( PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, "test" );
        // System.setProperty( PropertiesBasedJdbcDatabaseTester.DBUNIT_SCHEMA, "" );

        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/khoi_tracker_test");
        stmt = conn.createStatement();
    }

    protected IDataSet getDataSet() throws Exception
    {
        return new FlatXmlDataSetBuilder().build(new FileInputStream("user.xml"));
    }

    protected DatabaseOperation getSetUpOperation() throws Exception
    {
        return DatabaseOperation.REFRESH;
    }

    protected DatabaseOperation getTearDownOperation() throws Exception
    {
        return DatabaseOperation.DELETE_ALL;
    }

    @Test
    public void testNumberOfUsers() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM users");
        rs.next();

        assertThat(rs.getInt("count"), is(3));
    }

    @Test
    public void testFindUser() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = 2");
        rs.next();

        assertThat(rs.getString("first_name"), is("huy"));
        assertThat(rs.getString("last_name"), is("du"));
        assertThat(rs.getString("telegram_id"), is("321"));
        assertThat(rs.getString("user_name"), is("222"));
    }

    @Test
    public void testDeleteUser() throws SQLException {
        stmt.execute("DELETE FROM users WHERE id = 2");
        ResultSet rs = stmt.executeQuery("SELECT id FROM users ORDER BY id");

        rs.next();
        assertThat(rs.getString("id"), is("1"));

        rs.next();
        assertThat(rs.getString("id"), is("3"));

        assertThat(rs.next(), is(false));
    }
}
