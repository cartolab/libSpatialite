package spatialite;

import java.awt.geom.Rectangle2D;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.SQLiteConfig;

import com.iver.cit.gvsig.fmap.core.IFeature;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.IConnection;
import com.iver.cit.gvsig.fmap.drivers.IFeatureIterator;

public class test {


	public static void main(String[] args) {
		retrievingAllFields();

	}

	//It works!
	private static void gettingFullExtent() {
		try {
			SpatiaLiteDriver driver = new SpatiaLiteDriver();
			DBLayerDefinition lyrDef = initLayerSpatiaLite();
			driver.setData(lyrDef.getConnection(), lyrDef);
			Rectangle2D extent = driver.getFullExtent();
			System.out.println("FULL EXTENT: Max X: " + extent.getMaxX() +
					" | Max Y: " + extent.getMaxY() + " | Min X: " + extent.getMinX() +
					" | Min Y: " + extent.getMinY());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//It works!
	private static void readingSinglePointGeom() {
		try {
			SpatiaLiteDriver driver = new SpatiaLiteDriver();
			DBLayerDefinition lyrDef = initLayerSpatiaLite();
			driver.setData(lyrDef.getConnection(), lyrDef);
			IFeatureIterator features = driver.getFeatureIterator(driver.getSqlTotal());
			while (features.hasNext()) {
				IFeature feature = features.next();
				System.out.println("Geom: " + feature.getGeometry().toJTSGeometry().toText() +
						" | pk_uid: " + feature.getAttribute(0) +
						" | cod_com: " + feature.getAttribute(1) +
						" | departa: " + feature.getAttribute(2) +
						" | test1: " + feature.getAttribute(3));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	private static void retrievingAllFields() {
		try {
			SQLiteConfig config = new SQLiteConfig();
			config.enableLoadExtension(true);
			SpatiaLiteDriver driver = new SpatiaLiteDriver();
			Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			IConnection iconn = new ConnectionJDBC();
			((ConnectionJDBC) iconn).setDataConnection(conn, "", "");
			String[] fields = driver.getAllFields(iconn, "comunidad");
			for(String field: fields) {
				System.out.println("Campo de 'comunidad': " + field);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	private static void retrievingAllFieldTypes() {
		try {
			SQLiteConfig config = new SQLiteConfig();
			config.enableLoadExtension(true);
			SpatiaLiteDriver driver = new SpatiaLiteDriver();
			Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			IConnection iconn = new ConnectionJDBC();
			((ConnectionJDBC) iconn).setDataConnection(conn, "", "");
			String[] types = driver.getAllFieldTypeNames(iconn, "comunidad");
			for(String type: types) {
				System.out.println("Tipo de campo de 'comunidad': " + type);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	private static void retrievingGeometryField() {
		try {
			SQLiteConfig config = new SQLiteConfig();
			config.enableLoadExtension(true);
			SpatiaLiteDriver driver = new SpatiaLiteDriver();
			Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			IConnection iconn = new ConnectionJDBC();
			((ConnectionJDBC) iconn).setDataConnection(conn, "", "");
			String[] fields = driver.getGeometryFieldsCandidates(iconn, "comunidad");
			for(String field: fields) {
				System.out.println("Campo geometría de 'comunidad': " + field);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	private static void retrievingPrimaryKey() {
		try {
			SQLiteConfig config = new SQLiteConfig();
			config.enableLoadExtension(true);
			SpatiaLiteDriver driver = new SpatiaLiteDriver();
			Connection conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			IConnection iconn = new ConnectionJDBC();
			((ConnectionJDBC) iconn).setDataConnection(conn, "", "");
			String pk = driver.getPrimaryKey(iconn, "comunidad");
			System.out.println("Primary Key de 'comunidad': " + pk);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	private static void simpleTest() {
		SQLiteConfig config = new SQLiteConfig();
		config.enableLoadExtension(true);
		Connection conn;
		try {
			conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			ResultSet result = stmt.executeQuery("SELECT cod_com FROM comunidad;");
			while (result.next()) {
				System.out.println(result.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static DBLayerDefinition initLayerSpatiaLite()
	{
		SQLiteConfig config = new SQLiteConfig();
		config.enableLoadExtension(true);
        String dbURL = "jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite"; // latin1 is the catalog name
        String user = "";
        String pwd = "";
        String layerName = "comunidad";
        String tableName = "comunidad";
        IConnection conn;
		try {
			conn = new ConnectionJDBC();
			Connection realConnection = DriverManager.getConnection(dbURL, config.toProperties());
			Statement stmt = realConnection.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
			stmt.close();
			((ConnectionJDBC) conn).setDataConnection(realConnection, user, pwd);
			((ConnectionJDBC)conn).getConnection().setAutoCommit(false);

	        String fidField = "PK_UID";
	        String geomField = "geometry";

	        String[] fields = new String[4];
	        fields[0] = "PK_UID";
	        fields[1] = "cod_com";
	        fields[2] = "departa";
	        fields[3] = "test6";

	        String whereClause = "";

	        String strEPSG = "32616";
	        DBLayerDefinition lyrDef = new DBLayerDefinition();
	        lyrDef.setConnection(conn);
	        lyrDef.setName(layerName);
	        lyrDef.setTableName(tableName);
	        lyrDef.setWhereClause(whereClause);
	        lyrDef.setFieldNames(fields);
	        lyrDef.setFieldGeometry(geomField);
	        lyrDef.setFieldID(fidField);

	        lyrDef.setSRID_EPSG(strEPSG);
	        return lyrDef;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

}
