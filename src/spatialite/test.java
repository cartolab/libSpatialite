package spatialite;

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
		readingSinglePointGeom();

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
						" | departa: " + feature.getAttribute(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//It works!
	public void simpleTest() {
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

	        String[] fields = new String[3];
	        fields[0] = "PK_UID";
	        fields[1] = "cod_com";
	        fields[2] = "departa";

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
