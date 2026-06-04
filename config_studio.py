import sys
import os
import ruamel.yaml
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QGroupBox, QFormLayout, QLineEdit, QComboBox,
    QCheckBox, QPushButton, QFileDialog, QMessageBox, QLabel, QSpinBox
)
from PyQt6.QtCore import Qt

class ConfigStudio(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Migrator Config Studio")
        self.resize(800, 600)
        self.yaml = ruamel.yaml.YAML()
        self.yaml.preserve_quotes = True
        self.config_data = {}
        self.current_file = None

        self.init_ui()

    def init_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_connections_tab(), "Database Connections")
        self.tabs.addTab(self.create_settings_tab(), "Settings & Validators")
        main_layout.addWidget(self.tabs)

        # Bottom Action Bar
        action_layout = QHBoxLayout()
        self.btn_load = QPushButton("Load Config")
        self.btn_load.clicked.connect(self.load_config)
        self.btn_save = QPushButton("Save Config")
        self.btn_save.clicked.connect(self.save_config)
        
        self.status_label = QLabel("No configuration loaded.")
        
        action_layout.addWidget(self.btn_load)
        action_layout.addWidget(self.btn_save)
        action_layout.addStretch()
        action_layout.addWidget(self.status_label)

        main_layout.addLayout(action_layout)
        self.setCentralWidget(main_widget)

    def create_connections_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Source Connection
        source_group = QGroupBox("Source Database")
        s_layout = QFormLayout(source_group)
        self.s_type = QComboBox()
        self.s_type.addItems(["postgresql", "oracle", "mssql"])
        self.s_host = QLineEdit()
        self.s_port = QSpinBox()
        self.s_port.setRange(1, 65535)
        self.s_db = QLineEdit()
        self.s_schema = QLineEdit()
        self.s_user = QLineEdit()
        self.s_pass = QLineEdit()
        self.s_pass.setEchoMode(QLineEdit.EchoMode.Password)
        
        s_btn_test = QPushButton("Test Source Connection")
        s_btn_test.clicked.connect(lambda: self.test_connection('source'))

        s_layout.addRow("Database Type:", self.s_type)
        s_layout.addRow("Host:", self.s_host)
        s_layout.addRow("Port:", self.s_port)
        s_layout.addRow("Database/Service:", self.s_db)
        s_layout.addRow("Schema:", self.s_schema)
        s_layout.addRow("Username:", self.s_user)
        s_layout.addRow("Password:", self.s_pass)
        s_layout.addRow("", s_btn_test)

        # Target Connection
        target_group = QGroupBox("Target Database")
        t_layout = QFormLayout(target_group)
        self.t_type = QComboBox()
        self.t_type.addItems(["postgresql"])
        self.t_host = QLineEdit()
        self.t_port = QSpinBox()
        self.t_port.setRange(1, 65535)
        self.t_db = QLineEdit()
        self.t_schema = QLineEdit()
        self.t_user = QLineEdit()
        self.t_pass = QLineEdit()
        self.t_pass.setEchoMode(QLineEdit.EchoMode.Password)
        
        t_btn_test = QPushButton("Test Target Connection")
        t_btn_test.clicked.connect(lambda: self.test_connection('target'))

        t_layout.addRow("Database Type:", self.t_type)
        t_layout.addRow("Host:", self.t_host)
        t_layout.addRow("Port:", self.t_port)
        t_layout.addRow("Database:", self.t_db)
        t_layout.addRow("Schema:", self.t_schema)
        t_layout.addRow("Username:", self.t_user)
        t_layout.addRow("Password:", self.t_pass)
        t_layout.addRow("", t_btn_test)

        layout.addWidget(source_group)
        layout.addWidget(target_group)
        layout.addStretch()
        return tab

    def create_settings_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Migration Settings
        mig_group = QGroupBox("Migration Workflow")
        m_layout = QFormLayout(mig_group)
        self.m_workflow = QComboBox()
        self.m_workflow.addItems(["standard", "mapping"])
        self.m_drop_schema = QCheckBox("Drop schema if exists")
        self.m_recreate_schema = QCheckBox("Recreate schema")
        
        m_layout.addRow("Workflow:", self.m_workflow)
        m_layout.addRow("", self.m_drop_schema)
        m_layout.addRow("", self.m_recreate_schema)

        # Validation Settings
        val_group = QGroupBox("Validator")
        v_layout = QFormLayout(val_group)
        self.v_row_counts = QCheckBox("Check Row Counts")
        self.v_table_checksums = QCheckBox("Check Table Checksums")
        self.v_random_sample = QCheckBox("Check Random Sample")
        self.v_lob_sizes = QCheckBox("Check LOB Sizes")
        self.v_sample_size = QSpinBox()
        self.v_sample_size.setRange(1, 100000)

        v_layout.addRow("", self.v_row_counts)
        v_layout.addRow("", self.v_table_checksums)
        v_layout.addRow("", self.v_random_sample)
        v_layout.addRow("", self.v_lob_sizes)
        v_layout.addRow("Random Sample Size:", self.v_sample_size)

        layout.addWidget(mig_group)
        layout.addWidget(val_group)
        layout.addStretch()
        return tab

    def load_config(self):
        file_name, _ = QFileDialog.getOpenFileName(self, "Open Config File", "", "YAML Files (*.yaml *.yml);;All Files (*)")
        if file_name:
            try:
                with open(file_name, 'r') as f:
                    self.config_data = self.yaml.load(f)
                self.current_file = file_name
                self.populate_ui()
                self.status_label.setText(f"Loaded: {os.path.basename(file_name)}")
            except Exception as e:
                QMessageBox.critical(self, "Error Loading File", str(e))

    def populate_ui(self):
        if not self.config_data: return
        
        # Source
        src = self.config_data.get('source', {})
        self.s_type.setCurrentText(src.get('type', 'postgresql'))
        self.s_host.setText(src.get('host', ''))
        self.s_port.setValue(src.get('port', 5432))
        self.s_db.setText(src.get('database', ''))
        self.s_schema.setText(src.get('schema', ''))
        self.s_user.setText(src.get('username', ''))
        self.s_pass.setText(src.get('password', ''))

        # Target
        tgt = self.config_data.get('target', {})
        self.t_type.setCurrentText(tgt.get('type', 'postgresql'))
        self.t_host.setText(tgt.get('host', ''))
        self.t_port.setValue(tgt.get('port', 5432))
        self.t_db.setText(tgt.get('database', ''))
        self.t_schema.setText(tgt.get('schema', ''))
        self.t_user.setText(tgt.get('username', ''))
        self.t_pass.setText(tgt.get('password', ''))

        # Migration
        mig = self.config_data.get('migration', {})
        self.m_workflow.setCurrentText(mig.get('workflow', 'standard'))
        self.m_drop_schema.setChecked(mig.get('drop_schema', False))
        self.m_recreate_schema.setChecked(mig.get('recreate_schema', False))

        # Validator
        val = self.config_data.get('validator', {})
        self.v_row_counts.setChecked(val.get('check_row_counts', False))
        self.v_table_checksums.setChecked(val.get('check_table_checksums', False))
        self.v_random_sample.setChecked(val.get('check_random_sample', False))
        self.v_lob_sizes.setChecked(val.get('check_lob_sizes', False))
        self.v_sample_size.setValue(val.get('random_sample_size', 1000))

    def update_data_from_ui(self):
        if not self.config_data:
            self.config_data = {}
            
        # Ensure blocks exist
        for block in ['source', 'target', 'migration', 'validator']:
            if block not in self.config_data:
                self.config_data[block] = {}

        # Source
        src = self.config_data['source']
        src['type'] = self.s_type.currentText()
        src['host'] = self.s_host.text()
        src['port'] = self.s_port.value()
        src['database'] = self.s_db.text()
        src['schema'] = self.s_schema.text()
        src['username'] = self.s_user.text()
        src['password'] = self.s_pass.text()

        # Target
        tgt = self.config_data['target']
        tgt['type'] = self.t_type.currentText()
        tgt['host'] = self.t_host.text()
        tgt['port'] = self.t_port.value()
        tgt['database'] = self.t_db.text()
        tgt['schema'] = self.t_schema.text()
        tgt['username'] = self.t_user.text()
        tgt['password'] = self.t_pass.text()

        # Migration
        mig = self.config_data['migration']
        mig['workflow'] = self.m_workflow.currentText()
        mig['drop_schema'] = self.m_drop_schema.isChecked()
        mig['recreate_schema'] = self.m_recreate_schema.isChecked()

        # Validator
        val = self.config_data['validator']
        val['check_row_counts'] = self.v_row_counts.isChecked()
        val['check_table_checksums'] = self.v_table_checksums.isChecked()
        val['check_random_sample'] = self.v_random_sample.isChecked()
        val['check_lob_sizes'] = self.v_lob_sizes.isChecked()
        val['random_sample_size'] = self.v_sample_size.value()

    def save_config(self):
        if not self.current_file:
            self.current_file, _ = QFileDialog.getSaveFileName(self, "Save Config File", "", "YAML Files (*.yaml *.yml);;All Files (*)")
            if not self.current_file: return

        self.update_data_from_ui()

        # Ensure documentation comments are injected dynamically if missing
        if 'schema_mapping' not in self.config_data and 'mapping_workflow' not in self.config_data:
            self.config_data.yaml_set_comment_before_after_key('validator', before='\n# --- MIGRATOR SETTINGS END ---')

        try:
            with open(self.current_file, 'w') as f:
                self.yaml.dump(self.config_data, f)
            QMessageBox.information(self, "Success", f"Configuration saved successfully to {self.current_file}")
            self.status_label.setText(f"Saved: {os.path.basename(self.current_file)}")
        except Exception as e:
            QMessageBox.critical(self, "Error Saving File", str(e))

    def test_connection(self, db_side):
        if db_side == 'source':
            db_type = self.s_type.currentText()
            host = self.s_host.text()
            port = self.s_port.value()
            db = self.s_db.text()
            user = self.s_user.text()
            password = self.s_pass.text()
        else:
            db_type = self.t_type.currentText()
            host = self.t_host.text()
            port = self.t_port.value()
            db = self.t_db.text()
            user = self.t_user.text()
            password = self.t_pass.text()

        try:
            if db_type == 'postgresql':
                import psycopg2
                conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
                conn.close()
            elif db_type == 'oracle':
                import oracledb
                dsn = f"{host}:{port}/{db}"
                conn = oracledb.connect(user=user, password=password, dsn=dsn)
                conn.close()
            else:
                QMessageBox.warning(self, "Warning", f"Testing connectivity for {db_type} is currently not implemented in this GUI demo.")
                return

            QMessageBox.information(self, "Connection Success", f"Successfully connected to the {db_side} database!")
        except Exception as e:
            QMessageBox.critical(self, "Connection Failed", f"Could not connect to the {db_side} database:\n{str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ConfigStudio()
    window.show()
    sys.exit(app.exec())
