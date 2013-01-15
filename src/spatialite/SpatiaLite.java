/*
 * Created on 26-oct-2005
 *
 * gvSIG. Sistema de Información Geográfica de la Generalitat Valenciana
 *
 * Copyright (C) 2004 IVER T.I. and Generalitat Valenciana.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * For more information, contact:
 *
 *  Generalitat Valenciana
 *   Conselleria d'Infraestructures i Transport
 *   Av. Blasco Ibáñez, 50
 *   46010 VALENCIA
 *   SPAIN
 *
 *      +34 963862235
 *   gvsig@gva.es
 *      www.gvsig.gva.es
 *
 *    or
 *
 *   IVER T.I. S.A
 *   Salamanca 50
 *   46005 Valencia
 *   Spain
 *
 *   +34 963163400
 *   dac@iver.es
 */
package spatialite;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;

import com.hardcode.gdbms.engine.values.DateValue;
import com.hardcode.gdbms.engine.values.NullValue;
import com.hardcode.gdbms.engine.values.Value;
import com.hardcode.gdbms.engine.values.ValueWriter;
import com.iver.cit.gvsig.exceptions.visitors.ProcessVisitorException;
import com.iver.cit.gvsig.fmap.core.FPolygon2D;
import com.iver.cit.gvsig.fmap.core.FPolyline2D;
import com.iver.cit.gvsig.fmap.core.FShape;
import com.iver.cit.gvsig.fmap.core.FShape3D;
import com.iver.cit.gvsig.fmap.core.FShapeM;
import com.iver.cit.gvsig.fmap.core.GeneralPathX;
import com.iver.cit.gvsig.fmap.core.IFeature;
import com.iver.cit.gvsig.fmap.core.IGeometry;
import com.iver.cit.gvsig.fmap.core.IGeometry3D;
import com.iver.cit.gvsig.fmap.core.IGeometryM;
import com.iver.cit.gvsig.fmap.core.IRow;
import com.iver.cit.gvsig.fmap.core.ShapeFactory;
import com.iver.cit.gvsig.fmap.core.ShapeMFactory;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.DefaultJDBCDriver;
import com.iver.cit.gvsig.fmap.drivers.FieldDescription;
import com.iver.cit.gvsig.fmap.drivers.XTypes;
import com.iver.utiles.console.jedit.SQLTokenMarker;
import com.vividsolutions.jts.geom.Geometry;

/**
 * @author fjp
 * Necesitamos que esta clase no trabaje con funciones estáticas
 * porque puede haber capas que provengan de distintas bases de datos.
 */
public class SpatiaLite {

	private String toEncode;

	/**
	 * Mover esto a IverUtiles
	 *
	 * @param val
	 * @return
	 */
	public boolean isNumeric(Value val) {

		switch (val.getSQLType()) {
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.BIGINT:
		case Types.NUMERIC:
		case Types.REAL:
		case Types.TINYINT:
			return true;
		}

		return false;
	}

	public String getConnectionStringBeginning() {
		return "jdbc:sqlite:";
	}

	/**
	 * @param dbLayerDef
	 * @param fieldsDescr
	 * @param bCreateGID @DEPRECATED
	 * @return
	 */
	public String getSqlCreateSpatialTable(DBLayerDefinition dbLayerDef,
			FieldDescription[] fieldsDescr, boolean bCreateGID) {

		String resul;
		/* boolean bExistGID = false;
		for (int i = 0; i < dbLayerDef.getFieldNames().length; i++) {
			if (dbLayerDef.getFieldNames()[i].equalsIgnoreCase("gid")) {
				bExistGID = true;
				break;
			}
		} */
		/* if (bExistGID) // Usamos el existente y no añadimos ninguno nosotros
			resul = "CREATE TABLE " + dbLayerDef.getTableName() + " (";
		else */
		// FJP: NUEVO: NO TOLERAMOS CAMPOS QUE SE LLAMEN GID. Lo reservamos para uso nuestro.
		resul = "CREATE TABLE " + dbLayerDef.getComposedTableName()
					+ " ( " + "\""+dbLayerDef.getFieldID()+"\"" +" serial PRIMARY KEY ";
		int j=0;
		for (int i = 0; i < dbLayerDef.getFieldNames().length; i++) {
			int fieldType = fieldsDescr[i].getFieldType();
			String strType = XTypes.fieldTypeToString(fieldType);
			/*
			 * if (fieldType == Types.VARCHAR) strType = strType + "(" +
			 * fieldsDescr[i].getFieldLength() + ")";
			 */
			if (fieldsDescr[i].getFieldName().equalsIgnoreCase(dbLayerDef.getFieldID()))
				continue;
			resul = resul + ", " + "\""+dbLayerDef.getFieldNames()[i]+"\"" + " "	+ strType;
			j++;
		}
		resul = resul + ");";
		return resul;
	}

	public String getSqlAlterTable(DBLayerDefinition dbLayerDef) {
		String strGeometryFieldType;
		strGeometryFieldType = "GEOMETRY";

		switch (dbLayerDef.getShapeType()) {
		case FShape.POINT:
			strGeometryFieldType = XTypes.fieldTypeToString(XTypes.POINT2D);
			break;
		case FShape.LINE:
			strGeometryFieldType = XTypes.fieldTypeToString(XTypes.LINE2D);
			break;
		case FShape.POLYGON:
			strGeometryFieldType = XTypes.fieldTypeToString(XTypes.POLYGON2D);
			break;
		case FShape.MULTI:
			strGeometryFieldType = XTypes.fieldTypeToString(XTypes.MULTI2D);
			break;
		case FShape.MULTIPOINT:
			strGeometryFieldType = XTypes.fieldTypeToString(XTypes.MULTIPOINT);
			break;
		}

		String schema = dbLayerDef.getSchema();
		if (schema == null || schema.equals("")){
			schema = " current_schema()::Varchar ";
		} else {
			schema = "'" +schema + "'";
		}

		String result = "SELECT AddGeometryColumn("
				+ schema + ", '"
				+ dbLayerDef.getTableName() + "', '"
				+ dbLayerDef.getFieldGeometry() + "', "
				+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ", '" + strGeometryFieldType + "', "
				+ dbLayerDef.getDimension() + ");";

		return result;
	}

	/**
	 * From geotools Adds quotes to an object for storage in postgis. The object
	 * should be a string or a number. To perform an insert strings need quotes
	 * around them, and numbers work fine with quotes, so this method can be
	 * called on unknown objects.
	 *
	 * @param value
	 *            The object to add quotes to.
	 *
	 * @return a string representation of the object with quotes.
	 */
	protected String addQuotes(Object value) {
		String retString;

		if (value != null) {
			if (value instanceof NullValue)
				retString = "null";
			else
				retString = "'" + doubleQuote(value) + "'";

		} else {
			retString = "null";
		}

		return retString;
	}

	/**
	 * FIXME: Maybe we don't need to test if encoding to the database is possible or not. This conversion may be slow.
	 * But in the other hand, the user may be able to store his data and don't loose all the changes...
	 * @param obj
	 * @return
	 */
	private String doubleQuote(Object obj) {
		String aux = obj.toString().replaceAll("'", "''");
		StringBuffer strBuf = new StringBuffer(aux);
		ByteArrayOutputStream out = new ByteArrayOutputStream(strBuf.length());
		String aux2 = "Encoding ERROR";

		try {
			PrintStream printStream = new PrintStream(out, true, toEncode);
			printStream.print(aux);			
			aux2 = out.toString(toEncode);
//			System.out.println(aux + " " + aux2);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return aux2;
	}

	/**
	 * Based in code from JUMP (VividSolutions) and Geotools Things to be aware:
	 * We always will use Spatial Tables with Unique ID. IFeature has the same
	 * field order than dbLayerDef.getFieldNames()
	 *
	 * @param dbLayerDef
	 * @param feat
	 * @return
	 * @throws SQLException
	 * @throws ProcessVisitorException
	 */
	public String getSqlInsertFeature(DBLayerDefinition dbLayerDef,
			IFeature feat) throws ProcessVisitorException {
		StringBuffer sqlBuf = new StringBuffer("INSERT INTO "
				+ dbLayerDef.getComposedTableName() + " (");
		String sql = null;
		int numAlphanumericFields = dbLayerDef.getFieldNames().length;

		for (int i = 0; i < numAlphanumericFields; i++) {
			String name = dbLayerDef.getFieldsDesc()[i].getFieldName();
			// if (cols.contains(name) && (!name.equals(uniqueCol) ||
			// existsUnique)) {
			if (name.equals(dbLayerDef.getFieldID()))
				continue;
			sqlBuf.append(" " + "\""+name+"\"" + ",");
			// }
		}
		sqlBuf.append(" \"" + dbLayerDef.getFieldGeometry() + "\"");
		// sqlBuf.deleteCharAt(sqlBuf.lastIndexOf(","));
		sqlBuf.append(" ) VALUES (");
		String insertQueryHead = sqlBuf.toString();
		sqlBuf = new StringBuffer(insertQueryHead);
		for (int j = 0; j < numAlphanumericFields; j++) {
			String name = dbLayerDef.getFieldsDesc()[j].getFieldName();
			if (name.equals(dbLayerDef.getFieldID()))
				continue;

			if (isNumeric(feat.getAttribute(j))){
				sqlBuf.append(feat.getAttribute(j) + ", ");
			}else if(feat.getAttribute(j).getSQLType() == Types.DATE){
				//If the field is a date, the driver can not use the client encoding.
				//It uses the same encoding that the user has written on the table
				sqlBuf.append(addQuotes(((DateValue)feat.getAttribute(j)).getValue().toString()) + ", ");
			}else{
				sqlBuf.append(addQuotes(feat.getAttribute(j)) + ", ");
			}
		}
		IGeometry geometry=feat.getGeometry();
		int type=dbLayerDef.getShapeType();
		if (geometry.getGeometryType()!=type){
			if (type==FShape.POLYGON){
				geometry=ShapeFactory.createPolygon2D(new GeneralPathX(geometry.getInternalShape()));
			}else if (type==FShape.LINE){
				geometry=ShapeFactory.createPolyline2D(new GeneralPathX(geometry.getInternalShape()));
			}else if (type==(FShape.POLYGON|FShape.Z)){
				geometry=ShapeFactory.createPolygon3D(new GeneralPathX(geometry.getInternalShape()),((IGeometry3D)geometry).getZs());
			}else if (type==(FShape.LINE|FShape.Z)){
				geometry=ShapeFactory.createPolyline3D(new GeneralPathX(geometry.getInternalShape()),((IGeometry3D)geometry).getZs());
			}else if (type==(FShape.LINE|FShape.M)){ //MCoord
				geometry=ShapeMFactory.createPolyline2DM(new GeneralPathX(geometry.getInternalShape()),((IGeometryM)geometry).getMs()); //MCoord
			}
		}
		
		if (!isCorrectGeometry(geometry, type))
			throw new ProcessVisitorException("incorrect_geometry",new Exception());
		//MCoord
		if (((type & FShape.M) != 0) && ((type & FShape.MULTIPOINT) == 0)) {
			sqlBuf.append(" GeometryFromText( '"
					+ ((FShapeM)geometry.getInternalShape()).toText() + "', "
					+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ")");
		} else
			//ZCoord
			if ((type & FShape.Z) != 0) {
				if ((type & FShape.MULTIPOINT) != 0) {
					//TODO: Metodo toText 3D o 2DM 					
				} else {
				//Its not a multipoint
				sqlBuf.append(" GeometryFromText( '"
						+ ((FShape3D)feat.getGeometry().getInternalShape()).toText() + "', "
						+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ")");
				}

			}	
			//XYCoord
			else {
				Geometry jtsGeom=geometry.toJTSGeometry();
				if (jtsGeom==null || !isCorrectType(jtsGeom, type)){
					throw new ProcessVisitorException("incorrect_geometry",new Exception());
				}
				
		
			//If they layer is a 2D layer writing a 2D geometry will throw an error
			//With st_force_3d it is avoid		
			if (dbLayerDef.getDimension() == 3) sqlBuf.append("ST_Force_3D (");
			
			sqlBuf.append(" GeometryFromText( '"
				+ jtsGeom.toText() + "', "
				+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ")");

			if (dbLayerDef.getDimension() == 3) sqlBuf.append(")");			
		}

		// sqlBuf.deleteCharAt(sqlBuf.lastIndexOf(","));
		sqlBuf.append(" ) ");
		sql = sqlBuf.toString();
		return sql;
	}

	private boolean isCorrectType(Geometry jtsGeom, int type) {
		if (FShape.POLYGON==type){
			if (!jtsGeom.getGeometryType().equals("MultiPolygon") && !jtsGeom.getGeometryType().equals("Polygon") )
				return false;
		}
		return true;
	}

	private boolean isCorrectGeometry(IGeometry geometry, int type) {
		if (FShape.POLYGON==type){
			FPolygon2D polygon = (FPolygon2D)geometry.getInternalShape();
			if (!(polygon.getBounds2D().getWidth()>0 && polygon.getBounds2D().getHeight()>0))
				return false;
		}else if (FShape.LINE==type){
			FPolyline2D line = (FPolyline2D)geometry.getInternalShape();
			if (!(line.getBounds2D().getWidth()>0 || line.getBounds2D().getHeight()>0))
				return false;
		}

		return true;
	}

	public String getSqlCreateIndex(DBLayerDefinition lyrDef) {
		String indexName = lyrDef.getTableName() + "_"
				+ lyrDef.getFieldGeometry() + "_gist";
		String sql = "CREATE INDEX \"" + indexName + "\" ON \""
				+ lyrDef.getComposedTableName() + "\" USING GIST (\""
				+ lyrDef.getFieldGeometry() + "\" GIST_GEOMETRY_OPS)";

		return sql;
	}

	public String getSqlModifyFeature(DBLayerDefinition dbLayerDef, IFeature feat) {
		/*
		 	UPDATE weather
		 	SET temp_hi = temp_hi - 2,  temp_lo = temp_lo - 2
		 	WHERE date > '1994-11-28';
		 */
		StringBuffer sqlBuf = new StringBuffer("UPDATE "
				+ dbLayerDef.getComposedTableName() + " SET");
		String sql = null;
		int numAlphanumericFields = dbLayerDef.getFieldsDesc().length;

		for (int i = 0; i < numAlphanumericFields; i++) {
			FieldDescription fldDesc = dbLayerDef.getFieldsDesc()[i];
			if (fldDesc != null)
			{
				String name = fldDesc.getFieldName();
				// El campo gid no lo actualizamos.
				if (name.equalsIgnoreCase(dbLayerDef.getFieldID()))
					continue;
				Value val = feat.getAttribute(i);
				if (val != null)
				{
					String strAux = val.getStringValue(ValueWriter.internalValueWriter);
					sqlBuf.append(" " + "\""+name+"\"" + " = " + strAux + " ,");
				}
			}
		}
		//If pos > 0 there is at least one field..
		int pos = sqlBuf.lastIndexOf(",");
		if (pos > -1){
			sqlBuf.deleteCharAt(pos);
		}
		if (feat.getGeometry() != null)
		{
			if (pos > -1){
				sqlBuf.append(",");
			}
			sqlBuf.append(" \"" + dbLayerDef.getFieldGeometry() + "\"");
			sqlBuf.append(" = ");
			//MCoord
			int type = feat.getGeometry().getGeometryType();
			if (((type & FShape.M) != 0) && ((type & FShape.MULTIPOINT) == 0)) {
				sqlBuf.append(" GeometryFromText( '"
						+ ((FShapeM)feat.getGeometry().getInternalShape()).toText() + "', "
						+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ")");
			} else
				
			//ZCoord
				if ((type & FShape.Z) != 0) {
					if ((type & FShape.MULTIPOINT) != 0) {
						//TODO: Metodo toText 3D o 2DM 											
					} else {
					//Its not a multipoint
					sqlBuf.append(" GeometryFromText( '"
							+ ((FShape3D)feat.getGeometry().getInternalShape()).toText() + "', "
							+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ")");
					}
					
				}
			
			//XYCoord
			else{
				
				//If they layer is a 2D layer writing a 2D geometry will throw an error
				//With st_force_3d it is avoid		
				if (dbLayerDef.getDimension() == 3) sqlBuf.append("ST_Force_3D (");
				
				sqlBuf.append(" GeometryFromText( '"
				+ feat.getGeometry().toJTSGeometry().toText() + "', "
				+ DefaultJDBCDriver.removePrefix(dbLayerDef.getSRID_EPSG()) + ")");
				
				if (dbLayerDef.getDimension() == 3) sqlBuf.append(")");
			}
		}
		sqlBuf.append(" WHERE ");
		sqlBuf.append(dbLayerDef.getFieldID() + " = " + feat.getID());
		sql = sqlBuf.toString();
		return sql;

	}

	/**
	 * TODO: NECESITAMOS OTRO MÉTODO PARA BORRAR CORRECTAMENTE.
	 *	 Esto provocará errores, ya que getID que viene en un row no
	 *	 nos sirve dentro de un writer para modificar y/o borrar entidades
	 *	 Por ahora, cojo el ID del campo que me indica el dbLayerDef
	 * @param dbLayerDef
	 * @param row
	 * @return
	 */
	public String getSqlDeleteFeature(DBLayerDefinition dbLayerDef, IRow row) {
		// DELETE FROM weather WHERE city = 'Hayward';
		// TODO: NECESITAMOS OTRO MÉTODO PARA BORRAR CORRECTAMENTE.
		// Esto provocará errores, ya que getID que viene en un row no
		// nos sirve dentro de un writer para modificar y/o borrar entidades
		// Por ahora, cojo el ID del campo que me indica el dbLayerDev
		StringBuffer sqlBuf = new StringBuffer("DELETE FROM "
				+ dbLayerDef.getComposedTableName() + " WHERE ");
		String sql = null;
		int indexFieldId = dbLayerDef.getIdFieldID();
		sqlBuf.append("\""+dbLayerDef.getFieldID()+"\"" + " = " + row.getAttribute(indexFieldId));
		sql = sqlBuf.toString();

		return sql;
	}

	public String getEncoding() {
		return toEncode;
	}
	public void setEncoding(String toEncode){
		if (toEncode.compareToIgnoreCase("SQL_ASCII") == 0) {
			this.toEncode = "ASCII";
		} else if (toEncode.compareToIgnoreCase("WIN1252") == 0) {
			this.toEncode = "Latin1";
		} else if (toEncode.compareToIgnoreCase("UTF8") == 0) {
			this.toEncode = "UTF-8";
		} else {
			this.toEncode = toEncode;
		}
	}

	static String escapeFieldName(String name){
		if (!name.toLowerCase().equals(name)){
			return "\""+name.trim()+"\"";
		}
		if (!name.matches("[a-z][\\d\\S\\w]*")){
			return "\""+name.trim()+"\"";
		}
		if (name.indexOf(":")>0){
			return "\""+name.trim()+"\"";
		}
		//si es una palabra reservada lo escapamos
		if (SQLiteReservedWords.isReserved(name)){
			return "\""+name.trim()+"\"";
		}
		return name;
	}
}
