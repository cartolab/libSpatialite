package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A couple of tests for documentation purposes mostly. No gvSIG classes should
 * be used in these tests. Are used to check sqlite-jdbc driver in isolation.
 * libSpatialite classes can be used as helpers
 */
public class JavaTests {

    private String conStr;

    @BeforeClass
    public static void setUpBeforeClass() {
	NativeDependencies.libsPath = TestUtils.libPath;
    }

    @Before
    public void setUp() throws IOException {
	// Class.forName("org.sqlite.JDBC");
	File tmp = File.createTempFile("test", ".sqlite");
	tmp.deleteOnExit();
	conStr = "jdbc:sqlite:" + tmp.getAbsolutePath();
    }

    @Test
    /**
     * Tests that a new sqlite file can be created on disk just with opening a connection to it
     */
    public void createNewDatabase() throws Exception {

	Connection conn = DriverManager.getConnection(conStr);
	Statement st = conn.createStatement();
	ResultSet rs = st.executeQuery("SELECT 1");
	assertTrue("At least one row in the resultset", rs.next());
	assertEquals(1, rs.getInt(1));
	assertFalse("Only one row in the resultset", rs.next());
	rs.close();
	st.close();
	conn.close();
    }

    @Test
    /**
     * Probes that spatialite extension is loaded per connection and no per database
     */
    public void testExtensionIsLoadedPerCon() throws Exception {

	// DriverManager.getConnection creates a new sqlite "CoreConnection"
	// each time is invoked
	Connection con = DriverManager.getConnection(conStr);
	Connection conSpatial = DriverManager.getConnection(conStr);

	try {
	    Statement st = con.createStatement();
	    st.executeQuery("SELECT spatialite_version();");
	} catch (SQLException e) {
	    assertTrue(e.toString().contains(
		    "no such function: spatialite_version"));
	}

	NativeDependencies nativeDeps = new NativeDependencies();
	nativeDeps.loadSystemDependencies();
	nativeDeps.loadSpatialite(conSpatial);

	Statement stSpatial = conSpatial.createStatement();
	ResultSet rsSpatial = stSpatial
		.executeQuery("SELECT spatialite_version();");
	rsSpatial.next();
	assertEquals("4.3.0a", rsSpatial.getString(1));

	try {
	    Statement st = con.createStatement();
	    st.executeQuery("SELECT spatialite_version();");
	} catch (SQLException e) {
	    assertTrue(e.toString().contains(
		    "no such function: spatialite_version"));
	}
    }

    @Test
    public void testHowBatchInsertWorks() throws Exception {

	Connection conn = DriverManager.getConnection(conStr);
	Statement st = conn.createStatement();
	st.executeUpdate("drop table if exists people;");
	st.executeUpdate("create table people (name, occupation);");
	PreparedStatement prep = conn
		.prepareStatement("insert into people values (?, ?);");

	prep.setString(1, "Gandhi");
	prep.setString(2, "politics");
	prep.addBatch();
	prep.setString(1, "Turing");
	prep.setString(2, "computers");
	prep.addBatch();
	prep.setString(1, "Wittgenstein");
	prep.setString(2, "smartypants");
	prep.addBatch();

	conn.setAutoCommit(false);
	prep.executeBatch();
	conn.setAutoCommit(true);

	ResultSet rs = st.executeQuery("select * from people order by name;");
	rs.next();
	assertEquals("Gandhi", rs.getString("name"));
	assertEquals("politics", rs.getString("occupation"));

	rs.next();
	assertEquals("Turing", rs.getString("name"));
	assertEquals("computers", rs.getString("occupation"));

	rs.next();
	assertEquals("Wittgenstein", rs.getString("name"));
	assertEquals("smartypants", rs.getString("occupation"));

	assertFalse(rs.next());

	rs.close();
	st.close();
	conn.close();
    }

}
