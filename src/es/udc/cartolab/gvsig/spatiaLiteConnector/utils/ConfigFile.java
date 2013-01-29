/*
 * Copyright (c) 2010. CartoLab, Universidad de A Coruña
 * 
 * This file is part of extDBConnection
 * 
 * extDBConnection is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or any later version.
 * 
 * extDBConnection is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with extDBConnection.
 * If not, see <http://www.gnu.org/licenses/>.
*/
package es.udc.cartolab.gvsig.spatiaLiteConnector.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;



public class ConfigFile {

	private String preferencesFile;
	private static ConfigFile instance = null;
	private String file;

	private boolean fileExists = true;

	private ConfigFile() {
		String dir = System.getProperty("gvSIG.confDir");
		if (dir == null) {
			dir = System.getProperty("user.dir");
		}
		if (dir.endsWith(File.separator)) {
			preferencesFile = dir + "dbconnection.cfg";
		} else {
			preferencesFile = dir + File.separator + "dbconnection.cfg";
		}

		try {
			getProperties();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static ConfigFile getInstance() {
		if (instance == null) {
			instance = new ConfigFile();
		}
		return instance;
	}

	private void getProperties() throws IOException {
		File configFile = new File(preferencesFile);
		String line;
		try {
			//leer el archivo en busca de los datos
			BufferedReader fileReader = new BufferedReader(new FileReader(configFile));
			while ((line = fileReader.readLine())!=null) {
				if (line.charAt(0) != '#') {
					int spacePos = line.indexOf(" ");
					if (spacePos>0) {
						String configWord = line.substring(0, spacePos);
						if (configWord.toLowerCase().compareTo("server")==0) {
							//get value without the quotation marks
							file = line.substring(line.indexOf('"'));
							file = file.substring(1);
							file = file.substring(0, file.indexOf('"'));
						}
					}
				}
			}
		} catch (FileNotFoundException fnfe) {
			file = "";
			fileExists = false;
		}
	}

	public String getServer() {
		return file;
	}

	private void saveProperties() throws IOException {
		//save the file
		File configFile = new File(preferencesFile);
		if (!configFile.exists()) {
			configFile.createNewFile();
			fileExists = true;
		}
		if (configFile.canWrite()) {
			FileWriter fileWriter = new FileWriter(configFile);
			//TODO: añadir cabecera o descripción de los campos
			String[] lines = new String[5];
			lines[0] = "file \"" + file + "\"";
			for (int i = 0; i<lines.length; i++) {
				fileWriter.write(lines[i]+"\n");
			}
			fileWriter.flush();
		} else {
			throw new IOException("The file can not be edited");
		}
	}

	public void setProperties(String file) throws IOException {

		this.file = file;

		saveProperties();
	}

	public boolean fileExists() {
		return fileExists;
	}
}
