package spatialite;

import java.awt.geom.Rectangle2D;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConfig;

import com.hardcode.gdbms.driver.exceptions.ReadDriverException;
import com.hardcode.gdbms.engine.data.edition.DataWare;
import com.hardcode.gdbms.engine.values.Value;
import com.hardcode.gdbms.engine.values.ValueFactory;
import com.iver.cit.gvsig.fmap.core.FShape;
import com.iver.cit.gvsig.fmap.core.ICanReproject;
import com.iver.cit.gvsig.fmap.core.IGeometry;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.DefaultJDBCDriver;
import com.iver.cit.gvsig.fmap.drivers.DriverAttributes;
import com.iver.cit.gvsig.fmap.drivers.IConnection;
import com.iver.cit.gvsig.fmap.drivers.IFeatureIterator;
import com.iver.cit.gvsig.fmap.drivers.WKBParser3;
import com.iver.cit.gvsig.fmap.drivers.XTypes;
import com.iver.cit.gvsig.fmap.drivers.jdbc.postgis.PostGisDriver;

public class SQLiteDriver extends DefaultJDBCDriver implements ICanReproject{

	private static Logger logger = Logger.getLogger(SQLiteDriver.class
			.getName());
	private static final String NAME = "SQLite JDBC Driver";
	private static Connection conn = null;
	private WKBParser3 parser = new WKBParser3();
	private String originalEPSG = null;
	private static int FETCH_SIZE = 5000;
	private int fetch_min = -1;
	private int fetch_max = -1;
	private int myCursorId;
	private int currentPosition;
	private String sqlTotal;
	
	static {
		SQLiteConfig config = new SQLiteConfig();
		config.enableLoadExtension(true);
		try {
			conn = DriverManager.getConnection("jdbc:sqlite:/home/jlopez/spatialite/prueba.sqlite", config.toProperties());
			Statement stmt = conn.createStatement();
			stmt.execute("SELECT load_extension('/usr/lib/libspatialite.so.3.2.0');");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub

	}

	@Override
	public String getGeometryField(String fieldName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getDefaultPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getConnectionStringBeginning() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setData(IConnection conn, DBLayerDefinition lyrDef)
			throws DBException {
		// TODO Auto-generated method stub

	}

	@Override
	public DriverAttributes getDriverAttributes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWritable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getSqlTotal() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCompleteWhere() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IFeatureIterator getFeatureIterator(String sql) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IFeatureIterator getFeatureIterator(Rectangle2D r, String strEPSG) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IFeatureIterator getFeatureIterator(Rectangle2D r, String strEPSG,
			String[] alphaNumericFieldsNeeded) {
		// TODO Auto-generated method stub
		return null;
	}

	private void setAbsolutePosition(int index) throws SQLException {

		if (rs == null) {
			try {
				reload();
			}
			catch (Exception e) {
				e.printStackTrace();
				throw new SQLException(e);
			}
		}

		if ((index < fetch_min) || (index > fetch_max)) {
			// calculamos el intervalo correcto
			fetch_min = index;
			fetch_max = fetch_min + FETCH_SIZE - 1;
			// y cogemos ese cacho
			rs.close();
			
			Statement st = ((ConnectionJDBC)conn).getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			ResultSet.CONCUR_READ_ONLY);
			
			myCursorId++;
			st.execute("declare "
					+ getTableName()
					+ myCursorId
					+ "_wkb_cursorAbsolutePosition binary scroll cursor with hold for "
					+ sqlTotal);
			st.executeQuery("fetch absolute " + fetch_min + " in "
					+ getTableName() + myCursorId
					+ "_wkb_cursorAbsolutePosition");
			
			rs = st.executeQuery("fetch forward " + FETCH_SIZE + " in "
					+ getTableName() + myCursorId
					+ "_wkb_cursorAbsolutePosition");
			

		}
		rs.absolute(index - fetch_min + 1);
		currentPosition = index;

	}

	@Override
	public IGeometry getShape(int index) throws ReadDriverException {
		IGeometry geom = null;
		try {
			Statement stmt = conn.createStatement();
			String st = "SELECT asbinary(geometry) FROM comunidad WHERE pk_uid = " + index + ";";
			System.out.println("Query: " + st);
			ResultSet result = stmt.executeQuery(st);
			if (result != null) {
				byte[] data = result.getBytes(1);
				if (data == null) // null geometry.
					return null;
				geom = parser.parse(data);
			}
		} catch (SQLException e) {
			throw new ReadDriverException(this.getName(), e);
		}

		return geom;
	}

	public Value getFieldValue(long rowIndex, int idField)
	throws ReadDriverException {
		int index = (int) (rowIndex);
		try {
			setAbsolutePosition(index);
			int fieldId = idField + 2;
			return getFieldValue(rs, fieldId);
		} catch (SQLException e) {
			throw new ReadDriverException("SQLite Driver", e);
		}
	}

	private void getTableEPSG_and_shapeType(IConnection conn,
			DBLayerDefinition dbld) {
		try {
			Statement stAux = ((ConnectionJDBC) conn).getConnection()
					.createStatement();

			String sql  = "SELECT * FROM GEOMETRY_COLUMNS WHERE F_TABLE_NAME = '"
					+ dbld.getTableName() + "' AND F_GEOMETRY_COLUMN = '"
					+ dbld.getFieldGeometry() + "'";

			ResultSet rs = stAux.executeQuery(sql);
			if (rs.next()) {
				originalEPSG = "" + rs.getInt("SRID");
				String geometryType = rs.getString("TYPE");
				int shapeType = FShape.MULTI;
				if (geometryType.compareToIgnoreCase("POINT") == 0)
					shapeType = FShape.POINT;
				else if (geometryType.compareToIgnoreCase("LINESTRING") == 0)
					shapeType = FShape.LINE;
				else if (geometryType.compareToIgnoreCase("POLYGON") == 0)
					shapeType = FShape.POLYGON;
				else if (geometryType.compareToIgnoreCase("MULTIPOINT") == 0)
					shapeType = FShape.MULTIPOINT;
				else if (geometryType.compareToIgnoreCase("MULTILINESTRING") == 0)
					shapeType = FShape.LINE;
				else if (geometryType.compareToIgnoreCase("MULTILINESTRINGM") == 0) // MCoord
					shapeType = FShape.LINE | FShape.M;
				else if (geometryType.compareToIgnoreCase("MULTIPOLYGON") == 0)
					shapeType = FShape.POLYGON;
				dbld.setShapeType(shapeType);
				
				//jomarlla
				int dimension  = rs.getString("COORD_DIMENSION").length();
				dbld.setDimension(dimension);

			} else {
				originalEPSG = "-1";
			}

			rs.close();
		} catch (SQLException e) {
			originalEPSG = "-1";
			logger.error(e);
			e.printStackTrace();
		}

	}

	private Value getFieldValue(ResultSet aRs, int fieldId) throws SQLException {
		ResultSetMetaData metaData = aRs.getMetaData();
		byte[] byteBuf = aRs.getBytes(fieldId);
		if (byteBuf == null)
			return ValueFactory.createNullValue();
		else {
			ByteBuffer buf = ByteBuffer.wrap(byteBuf);
			if (metaData.getColumnType(fieldId) == Types.VARCHAR)
				return ValueFactory.createValue(aRs.getString(fieldId));
			if (metaData.getColumnType(fieldId) == Types.CHAR){
				String character = aRs.getString(fieldId);
				if (character != null){
					return ValueFactory.createValue(character.trim());
				}else{
					return ValueFactory.createValue(character);
				}
			}
			if (metaData.getColumnType(fieldId) == Types.FLOAT)
				return ValueFactory.createValue(buf.getFloat());
			if (metaData.getColumnType(fieldId) == Types.DOUBLE)
				return ValueFactory.createValue(buf.getDouble());
			if (metaData.getColumnType(fieldId) == Types.REAL)
				return ValueFactory.createValue(buf.getFloat());
			if (metaData.getColumnType(fieldId) == Types.INTEGER)
				return ValueFactory.createValue(buf.getInt());
			if (metaData.getColumnType(fieldId) == Types.SMALLINT)
				return ValueFactory.createValue(buf.getShort());
			if (metaData.getColumnType(fieldId) == Types.BIGINT)
				return ValueFactory.createValue(buf.getLong());
			if (metaData.getColumnType(fieldId) == Types.BIT)
				return ValueFactory.createValue((byteBuf[0] == 1));
			if (metaData.getColumnType(fieldId) == Types.BOOLEAN)
				return ValueFactory.createValue(aRs.getBoolean(fieldId));
			if (metaData.getColumnType(fieldId) == Types.DATE) {
				long daysAfter2000 = buf.getInt() + 1;
				long msecs = daysAfter2000 * 24 * 60 * 60 * 1000;
				long real_msecs_date1 = (long) (XTypes.NUM_msSecs2000 + msecs);
				Date realDate1 = new Date(real_msecs_date1);
				return ValueFactory.createValue(realDate1);
			}
			if (metaData.getColumnType(fieldId) == Types.TIME) {
				return ValueFactory.createValue("NOT IMPLEMENTED YET");
			}
			if (metaData.getColumnType(fieldId) == Types.TIMESTAMP) {
				double segsReferredTo2000 = buf.getDouble();
				long real_msecs = (long) (XTypes.NUM_msSecs2000 + segsReferredTo2000 * 1000);
				Timestamp valTimeStamp = new Timestamp(real_msecs);
				return ValueFactory.createValue(valTimeStamp);
			}
			if (metaData.getColumnType(fieldId) == Types.NUMERIC) {
//				BigDecimal dec;
//				dec = getBigDecimal(buf.array());
//				return ValueFactory.createValue(dec.doubleValue());
				throw new UnsupportedOperationException("SQLite NUMERIC TYPE");
			}

		}

		return ValueFactory.createNullValue();

	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public int[] getPrimaryKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write(DataWare dataWare) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSourceProjection(IConnection conn, DBLayerDefinition dbld) {
		if (originalEPSG == null)
			getTableEPSG_and_shapeType(conn, dbld);
		return originalEPSG;
	}

	@Override
	public String getDestProjection() {
		return null;
	}

	@Override
	public void setDestProjection(String toEPSG) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean canReproject(String toEPSGdestinyProjection) {
		return false;
	}

}
