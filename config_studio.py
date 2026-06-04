import sys
import os
import io
import ruamel.yaml
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QGroupBox, QFormLayout, QLineEdit, QComboBox,
    QCheckBox, QPushButton, QFileDialog, QMessageBox, QLabel, QSpinBox,
    QPlainTextEdit
)
from PyQt6.QtCore import Qt

class ConfigStudio(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Migrator Config Studio")
        self.resize(900, 700)
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
        self.tabs.addTab(self.create_environment_tab(), "Environment & Drivers")
        self.tabs.addTab(self.create_settings_tab(), "Settings & Validators")
        self.tabs.addTab(self.create_table_filters_tab(), "Table Filtering")
        self.tabs.addTab(self.create_advanced_tab(), "Advanced Configuration")
        
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

        # Migrator Metadata DB
        mig_group = QGroupBox("Migrator Metadata Database")
        mig_layout = QFormLayout(mig_group)
        self.mig_host = QLineEdit()
        self.mig_port = QSpinBox()
        self.mig_port.setRange(1, 65535)
        self.mig_db = QLineEdit()
        self.mig_schema = QLineEdit()
        self.mig_user = QLineEdit()
        self.mig_pass = QLineEdit()
        self.mig_pass.setEchoMode(QLineEdit.EchoMode.Password)
        mig_layout.addRow("Host:", self.mig_host)
        mig_layout.addRow("Port:", self.mig_port)
        mig_layout.addRow("Database:", self.mig_db)
        mig_layout.addRow("Schema:", self.mig_schema)
        mig_layout.addRow("Username:", self.mig_user)
        mig_layout.addRow("Password:", self.mig_pass)

        # Source Connection
        source_group = QGroupBox("Source Database")
        s_layout = QFormLayout(source_group)
        self.s_type = QComboBox()
        self.s_type.addItems(["postgresql", "oracle", "mssql", "informix", "sybase_ase", "ibm_db2_luw", "ibm_db2_zos", "mysql", "sql_anywhere"])
        self.s_conn = QComboBox()
        self.s_conn.addItems(["native", "jdbc", "odbc", "ddl"])
        self.s_host = QLineEdit()
        self.s_port = QSpinBox()
        self.s_port.setRange(1, 65535)
        self.s_db = QLineEdit()
        self.s_schema = QLineEdit()
        self.s_user = QLineEdit()
        self.s_pass = QLineEdit()
        self.s_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.s_system_catalog = QLineEdit()
        self.s_db_locale = QLineEdit()
        
        s_btn_test = QPushButton("Test Source Connection")
        s_btn_test.clicked.connect(lambda: self.test_connection('source'))

        s_layout.addRow("Database Type:", self.s_type)
        s_layout.addRow("Connectivity:", self.s_conn)
        s_layout.addRow("Host:", self.s_host)
        s_layout.addRow("Port:", self.s_port)
        s_layout.addRow("Database/Service:", self.s_db)
        s_layout.addRow("Schema:", self.s_schema)
        s_layout.addRow("Username:", self.s_user)
        s_layout.addRow("Password:", self.s_pass)
        s_layout.addRow("System Catalog:", self.s_system_catalog)
        s_layout.addRow("DB Locale:", self.s_db_locale)
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

        layout.addWidget(mig_group)
        layout.addWidget(source_group)
        layout.addWidget(target_group)
        layout.addStretch()
        return tab

    def create_environment_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Drivers
        drv_group = QGroupBox("Driver Libraries")
        d_layout = QFormLayout(drv_group)
        self.j_driver = QLineEdit()
        self.j_libs = QLineEdit()
        self.o_driver = QLineEdit()
        self.o_libs = QLineEdit()
        self.d_path = QLineEdit()

        d_layout.addRow("JDBC Driver:", self.j_driver)
        d_layout.addRow("JDBC Libraries:", self.j_libs)
        d_layout.addRow("ODBC Driver:", self.o_driver)
        d_layout.addRow("ODBC Libraries:", self.o_libs)
        d_layout.addRow("DDL Path:", self.d_path)

        # Environment Variables
        env_group = QGroupBox("Environment Variables (format: NAME=VALUE, one per line)")
        env_layout = QVBoxLayout(env_group)
        self.env_vars = QPlainTextEdit()
        env_layout.addWidget(self.env_vars)

        layout.addWidget(drv_group)
        layout.addWidget(env_group)
        layout.addStretch()
        return tab

    def create_settings_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Migration Settings
        mig_group = QGroupBox("Migration Workflow & Execution")
        m_layout = QFormLayout(mig_group)
        self.m_workflow = QComboBox()
        self.m_workflow.addItems(["standard", "mapping", "anonymization"])
        self.m_drop_schema = QCheckBox("Drop schema if exists")
        self.m_recreate_schema = QCheckBox("Recreate schema")
        
        self.m_batch_size = QSpinBox()
        self.m_batch_size.setRange(1, 10000000)
        self.m_chunk_size = QSpinBox()
        self.m_chunk_size.setRange(-1, 10000000)
        self.m_chunk_size.setSpecialValueText("Disabled (-1)")
        self.m_workers = QSpinBox()
        self.m_workers.setRange(1, 128)

        m_layout.addRow("Workflow:", self.m_workflow)
        m_layout.addRow("Batch Size:", self.m_batch_size)
        m_layout.addRow("Chunk Size:", self.m_chunk_size)
        m_layout.addRow("Parallel Workers:", self.m_workers)
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
        self.v_workers = QSpinBox()
        self.v_workers.setRange(1, 128)

        v_layout.addRow("", self.v_row_counts)
        v_layout.addRow("", self.v_table_checksums)
        v_layout.addRow("", self.v_random_sample)
        v_layout.addRow("", self.v_lob_sizes)
        v_layout.addRow("Validator Workers:", self.v_workers)
        v_layout.addRow("Random Sample Size:", self.v_sample_size)

        layout.addWidget(mig_group)
        layout.addWidget(val_group)
        layout.addStretch()
        return tab

    def create_table_filters_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        inc_group = QGroupBox("Include Tables (One regex per line, or 'all')")
        inc_layout = QVBoxLayout(inc_group)
        self.inc_tables = QPlainTextEdit()
        inc_layout.addWidget(self.inc_tables)

        exc_group = QGroupBox("Exclude Tables (One regex per line)")
        exc_layout = QVBoxLayout(exc_group)
        self.exc_tables = QPlainTextEdit()
        exc_layout.addWidget(self.exc_tables)

        layout.addWidget(inc_group)
        layout.addWidget(exc_group)
        return tab

    def create_advanced_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        adv_group = QGroupBox("Advanced YAML Editor (table_settings, mapping_workflow, anonymization)")
        adv_layout = QVBoxLayout(adv_group)
        self.adv_yaml = QPlainTextEdit()
        
        font = self.adv_yaml.font()
        font.setFamily("Monospace")
        self.adv_yaml.setFont(font)
        
        adv_layout.addWidget(self.adv_yaml)

        layout.addWidget(adv_group)
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
        
        # Migrator metadata
        mig_db = self.config_data.get('migrator', {})
        self.mig_host.setText(mig_db.get('host', ''))
        self.mig_port.setValue(mig_db.get('port', 5432))
        self.mig_db.setText(mig_db.get('database', ''))
        self.mig_schema.setText(mig_db.get('schema', ''))
        self.mig_user.setText(mig_db.get('username', ''))
        self.mig_pass.setText(mig_db.get('password', ''))

        # Source
        src = self.config_data.get('source', {})
        self.s_type.setCurrentText(src.get('type', 'postgresql'))
        self.s_conn.setCurrentText(src.get('connectivity', 'native'))
        self.s_host.setText(src.get('host', ''))
        self.s_port.setValue(src.get('port', 5432))
        self.s_db.setText(src.get('database', ''))
        self.s_schema.setText(src.get('schema', ''))
        self.s_user.setText(src.get('username', ''))
        self.s_pass.setText(src.get('password', ''))
        self.s_system_catalog.setText(src.get('system_catalog', ''))
        self.s_db_locale.setText(src.get('db_locale', ''))

        # Target
        tgt = self.config_data.get('target', {})
        self.t_type.setCurrentText(tgt.get('type', 'postgresql'))
        self.t_host.setText(tgt.get('host', ''))
        self.t_port.setValue(tgt.get('port', 5432))
        self.t_db.setText(tgt.get('database', ''))
        self.t_schema.setText(tgt.get('schema', ''))
        self.t_user.setText(tgt.get('username', ''))
        self.t_pass.setText(tgt.get('password', ''))

        # Env & Drivers
        jdbc = src.get('jdbc', {})
        self.j_driver.setText(jdbc.get('driver', ''))
        self.j_libs.setText(jdbc.get('libraries', ''))
        odbc = src.get('odbc', {})
        self.o_driver.setText(odbc.get('driver', ''))
        self.o_libs.setText(odbc.get('libraries', ''))
        ddl = src.get('ddl', {})
        self.d_path.setText(ddl.get('path', ''))

        env_vars = self.config_data.get('env_variables', [])
        if env_vars is None: env_vars = []
        env_str = "\n".join([f"{e.get('name', '')}={e.get('value', '')}" for e in env_vars])
        self.env_vars.setPlainText(env_str)

        # Migration Settings
        mig = self.config_data.get('migration', {})
        self.m_workflow.setCurrentText(mig.get('workflow', 'standard'))
        self.m_drop_schema.setChecked(mig.get('drop_schema', False))
        self.m_recreate_schema.setChecked(mig.get('recreate_schema', False))
        self.m_batch_size.setValue(mig.get('batch_size', 100000))
        self.m_chunk_size.setValue(mig.get('chunk_size', -1))
        self.m_workers.setValue(mig.get('parallel_workers', 4))

        # Validator
        val = self.config_data.get('validator', {})
        self.v_row_counts.setChecked(val.get('check_row_counts', False))
        self.v_table_checksums.setChecked(val.get('check_table_checksums', False))
        self.v_random_sample.setChecked(val.get('check_random_sample', False))
        self.v_lob_sizes.setChecked(val.get('check_lob_sizes', False))
        self.v_workers.setValue(val.get('workers', 4))
        self.v_sample_size.setValue(val.get('random_sample_size', 1000))

        # Include/Exclude
        inc = self.config_data.get('include_tables', [])
        if inc is None: inc = []
        if isinstance(inc, str):
            self.inc_tables.setPlainText(inc)
        else:
            self.inc_tables.setPlainText("\n".join(inc))

        exc = self.config_data.get('exclude_tables', [])
        if exc is None: exc = []
        if isinstance(exc, str):
            self.exc_tables.setPlainText(exc)
        else:
            self.exc_tables.setPlainText("\n".join(exc))

        # Advanced YAML Editor
        adv_dict = {}
        for key in ['table_settings', 'mapping_workflow', 'anonymization', 'scheduled_actions']:
            if key in self.config_data:
                adv_dict[key] = self.config_data[key]
        
        if adv_dict:
            stream = io.StringIO()
            self.yaml.dump(adv_dict, stream)
            self.adv_yaml.setPlainText(stream.getvalue())
        else:
            self.adv_yaml.setPlainText("")

    def update_data_from_ui(self):
        if not self.config_data:
            self.config_data = {}
            
        # Ensure core blocks exist
        for block in ['migrator', 'source', 'target', 'migration', 'validator']:
            if block not in self.config_data:
                self.config_data[block] = {}

        mig_db = self.config_data['migrator']
        mig_db['host'] = self.mig_host.text()
        mig_db['port'] = self.mig_port.value()
        mig_db['database'] = self.mig_db.text()
        mig_db['schema'] = self.mig_schema.text()
        mig_db['username'] = self.mig_user.text()
        mig_db['password'] = self.mig_pass.text()

        src = self.config_data['source']
        src['type'] = self.s_type.currentText()
        src['connectivity'] = self.s_conn.currentText()
        src['host'] = self.s_host.text()
        src['port'] = self.s_port.value()
        src['database'] = self.s_db.text()
        src['schema'] = self.s_schema.text()
        src['username'] = self.s_user.text()
        src['password'] = self.s_pass.text()
        if self.s_system_catalog.text(): src['system_catalog'] = self.s_system_catalog.text()
        if self.s_db_locale.text(): src['db_locale'] = self.s_db_locale.text()

        tgt = self.config_data['target']
        tgt['type'] = self.t_type.currentText()
        tgt['host'] = self.t_host.text()
        tgt['port'] = self.t_port.value()
        tgt['database'] = self.t_db.text()
        tgt['schema'] = self.t_schema.text()
        tgt['username'] = self.t_user.text()
        tgt['password'] = self.t_pass.text()

        # Env & Drivers
        if self.j_driver.text() or self.j_libs.text():
            src['jdbc'] = {'driver': self.j_driver.text(), 'libraries': self.j_libs.text()}
        if self.o_driver.text() or self.o_libs.text():
            src['odbc'] = {'driver': self.o_driver.text(), 'libraries': self.o_libs.text()}
        if self.d_path.text():
            src['ddl'] = {'path': self.d_path.text()}

        env_lines = [l.strip() for l in self.env_vars.toPlainText().split('\n') if l.strip()]
        env_list = []
        for line in env_lines:
            if '=' in line:
                k, v = line.split('=', 1)
                env_list.append({'name': k.strip(), 'value': v.strip()})
        if env_list:
            self.config_data['env_variables'] = env_list

        mig = self.config_data['migration']
        mig['workflow'] = self.m_workflow.currentText()
        mig['drop_schema'] = self.m_drop_schema.isChecked()
        mig['recreate_schema'] = self.m_recreate_schema.isChecked()
        mig['batch_size'] = self.m_batch_size.value()
        mig['chunk_size'] = self.m_chunk_size.value()
        mig['parallel_workers'] = self.m_workers.value()

        val = self.config_data['validator']
        val['check_row_counts'] = self.v_row_counts.isChecked()
        val['check_table_checksums'] = self.v_table_checksums.isChecked()
        val['check_random_sample'] = self.v_random_sample.isChecked()
        val['check_lob_sizes'] = self.v_lob_sizes.isChecked()
        val['workers'] = self.v_workers.value()
        val['random_sample_size'] = self.v_sample_size.value()

        # Filters
        inc_txt = self.inc_tables.toPlainText().strip()
        if inc_txt == 'all':
            self.config_data['include_tables'] = 'all'
        elif inc_txt:
            self.config_data['include_tables'] = [l.strip() for l in inc_txt.split('\n') if l.strip()]

        exc_txt = self.exc_tables.toPlainText().strip()
        if exc_txt:
            self.config_data['exclude_tables'] = [l.strip() for l in exc_txt.split('\n') if l.strip()]

        # Advanced
        adv_txt = self.adv_yaml.toPlainText().strip()
        if adv_txt:
            try:
                parsed_adv = self.yaml.load(adv_txt)
                if parsed_adv and isinstance(parsed_adv, dict):
                    for k, v in parsed_adv.items():
                        self.config_data[k] = v
            except Exception as e:
                raise Exception(f"Failed to parse Advanced YAML section: {str(e)}")

    def save_config(self):
        if not self.current_file:
            self.current_file, _ = QFileDialog.getSaveFileName(self, "Save Config File", "", "YAML Files (*.yaml *.yml);;All Files (*)")
            if not self.current_file: return

        try:
            self.update_data_from_ui()
            
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
