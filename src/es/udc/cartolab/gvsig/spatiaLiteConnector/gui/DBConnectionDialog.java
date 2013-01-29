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
package es.udc.cartolab.gvsig.spatiaLiteConnector.gui;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;

import javax.swing.AbstractButton;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import com.iver.andami.PluginServices;
import com.iver.cit.gvsig.ProjectExtension;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.jeta.forms.components.panel.FormPanel;

import es.udc.cartolab.gvsig.spatiaLiteConnector.utils.ConfigFile;
import es.udc.cartolab.gvsig.spatiaLiteConnector.utils.DBSession;

public class DBConnectionDialog  extends AbstractGVWindow {

	private final static int INIT_MIN_HEIGHT = 90;

	private JPanel centerPanel = null;

	private JTextField fileTF;
    private JButton dotsButton;

    public static final String ID_SQLITEFILETF = "sqliteFileTF";
	public static final String ID_SQLITEFILEL = "sqliteFileLabel";
    public static final String ID_SQLITEDOTSBTN = "sqliteDotsButton";

	public DBConnectionDialog() {
		super(425, INIT_MIN_HEIGHT);
		setTitle(PluginServices.getText(this, "Login"));
	}

	@Override
	protected JPanel getCenterPanel() {

		if (centerPanel == null) {
			centerPanel = new JPanel();
			FormPanel form = new FormPanel("forms/dbConnection.xml");
			centerPanel.add(form);
			fileTF = form.getTextField(ID_SQLITEFILETF);
			dotsButton = (JButton) form.getButton(ID_SQLITEDOTSBTN);
			dotsButton.addActionListener(this);

			// localization
			JLabel serverLabel = form.getLabel(ID_SQLITEFILEL);

			serverLabel.setText(PluginServices.getText(this, "sqlite_file"));

			DBSession dbs = DBSession.getCurrentSession();
			if (dbs != null) {
				fileTF.setText(dbs.getSQLiteFile());
			}

		}
		return centerPanel;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
        if (e.getSource() == dotsButton) {
            File currentDb = new File(fileTF.getText());
            JFileChooser chooser;
            if (currentDb.exists()) {
                chooser = new JFileChooser(currentDb);
            } else {
                chooser = new JFileChooser();
            }
            int returnVal = chooser.showOpenDialog(this);
            if (returnVal == JFileChooser.APPROVE_OPTION) {
            	fileTF.setText(chooser.getSelectedFile()
                        .getAbsolutePath());
            }
            return;
        } else {
        	super.actionPerformed(e);
        }
	}

	private void saveConfig(String file) throws IOException {
		// save config file
		ConfigFile cf = ConfigFile.getInstance();
		cf.setProperties(file);
		PluginServices.getMDIManager().restoreCursor();
		String title = " "
				+ String.format(PluginServices.getText(this, "connectedTitle"), file);
		PluginServices.getMainFrame().setTitle(title);
	}

	private boolean activeSession() throws DBException {

		DBSession dbs = DBSession.getCurrentSession();
		if (dbs != null) {
			if (!dbs.askSave()) {
				return false;
			}
			dbs.close();

			ProjectExtension pExt = (ProjectExtension) PluginServices
					.getExtension(ProjectExtension.class);
			pExt.execute("NUEVO");
		}
		return true;

	}

	@Override
	protected void onOK() {

		try {

			if (!activeSession()) {
				return;
			}

			PluginServices.getMDIManager().setWaitCursor();

			String file = fileTF.getText().trim();

			DBSession.createConnection(file);

			closeWindow();

			saveConfig(file);

		} catch (DBException e1) {
			// Login error
			e1.printStackTrace();
			PluginServices.getMDIManager().restoreCursor();
			JOptionPane.showMessageDialog(this,
					PluginServices.getText(this, "databaseConnectionError"),
					PluginServices.getText(this, "connectionError"),
					JOptionPane.ERROR_MESSAGE);

		} catch (IOException e3) {
			PluginServices.getMDIManager().restoreCursor();
	    PluginServices.getLogger().error(e3);
		}
	}

	@Override
	protected Component getDefaultFocusComponent() {
		return fileTF;
	}

}
