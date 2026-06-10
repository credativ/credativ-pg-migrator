import sys
import os
import io
import json
import ruamel.yaml
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QGroupBox, QFormLayout, QLineEdit, QComboBox,
    QCheckBox, QPushButton, QFileDialog, QMessageBox, QLabel, QSpinBox,
    QPlainTextEdit, QDialog, QFontComboBox, QDialogButtonBox, QSizePolicy
)
from PyQt6.QtCore import Qt, QByteArray
from PyQt6.QtGui import QCursor, QFont
import config_help
import config_studio_themes

class HelpPopup(QLabel):
    def __init__(self, text, parent=None):
        super().__init__(text, parent)
        self.setWindowFlags(Qt.WindowType.Popup)
        self.setObjectName("HelpPopup")
        self.setWordWrap(True)
        self.setMinimumWidth(300)
        self.setMaximumWidth(500)

    def mousePressEvent(self, event):
        self.hide()

class SettingsManager:
    def __init__(self, filename="config_studio_settings.json"):
        self.filename = filename
        self.settings = {
            "font_family": "",
            "font_size": 10,
            "theme": "VS Code Dark+",
            "window_geometry": ""
        }
        self.load()

    def load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    self.settings.update(data)
            except Exception:
                pass

    def save(self):
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.settings, f, indent=4)
        except Exception as e:
            print(f"Failed to save settings: {e}")

class SettingsDialog(QDialog):
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Studio Settings")
        self.settings_manager = settings_manager
        
        self.original_settings = self.settings_manager.settings.copy()
        
        layout = QFormLayout(self)
        
        self.font_cb = QFontComboBox()
        if settings_manager.settings["font_family"]:
            self.font_cb.setCurrentFont(QFont(settings_manager.settings["font_family"]))
        self.font_cb.currentFontChanged.connect(self.preview_settings)
            
        self.size_sb = QSpinBox()
        self.size_sb.setRange(8, 36)
        self.size_sb.setValue(settings_manager.settings["font_size"])
        self.size_sb.valueChanged.connect(self.preview_settings)
        
        self.theme_cb = QComboBox()
        self.theme_cb.addItems(list(config_studio_themes.THEMES.keys()))
        self.theme_cb.setCurrentText(settings_manager.settings["theme"])
        self.theme_cb.currentTextChanged.connect(self.preview_settings)
        
        layout.addRow("Font Family:", self.font_cb)
        layout.addRow("Font Size:", self.size_sb)
        layout.addRow("Color Theme:", self.theme_cb)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addRow("", btn_box)

    def preview_settings(self):
        self.settings_manager.settings["font_family"] = self.font_cb.currentFont().family()
        self.settings_manager.settings["font_size"] = self.size_sb.value()
        self.settings_manager.settings["theme"] = self.theme_cb.currentText()
        if self.parent():
            self.parent().apply_settings()

    def reject(self):
        self.settings_manager.settings = self.original_settings.copy()
        if self.parent():
            self.parent().apply_settings()
        super().reject()

    def accept(self):
        self.settings_manager.settings["font_family"] = self.font_cb.currentFont().family()
        self.settings_manager.settings["font_size"] = self.size_sb.value()
        self.settings_manager.settings["theme"] = self.theme_cb.currentText()
        self.settings_manager.save()
        super().accept()

class ConfigStudio(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Migrator Config Studio")
        self.resize(900, 700)
        self.yaml = ruamel.yaml.YAML()
        self.yaml.preserve_quotes = True
        self.config_data = {}
        self.current_file = None

        self.settings_manager = SettingsManager()
        self.init_ui()
        self.apply_settings()
        
        geom_hex = self.settings_manager.settings.get("window_geometry")
        if geom_hex:
            self.restoreGeometry(QByteArray.fromHex(geom_hex.encode('utf-8')))

    def init_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_connections_tab(), "Database Connections")
        self.tabs.addTab(self.create_environment_tab(), "Environment & Drivers")
        self.tabs.addTab(self.create_settings_tab(), "Workflow settings")
        self.tabs.addTab(self.create_validator_tab(), "Validator")
        self.tabs.addTab(self.create_premigration_tab(), "Premigration Analysis")
        self.tabs.addTab(self.create_data_export_tab(), "Data Export")
        self.tabs.addTab(self.create_table_filters_tab(), "Table Filtering")
        self.tabs.addTab(self.create_advanced_tab(), "Advanced Configuration")
        
        main_layout.addWidget(self.tabs)
        
        # Sync the two connectivity combo boxes
        self.s_conn.currentTextChanged.connect(self.s_conn_env.setCurrentText)
        self.s_conn_env.currentTextChanged.connect(self.s_conn.setCurrentText)
        
        # Ensure mutually exclusive driver inputs
        self.s_conn_env.currentTextChanged.connect(self.on_connectivity_changed)
        self.on_connectivity_changed(self.s_conn_env.currentText())

        # Bottom Action Bar
        action_layout = QHBoxLayout()
        self.btn_load = QPushButton("Load Config")
        self.btn_load.clicked.connect(self.load_config)
        self.btn_settings = QPushButton("Studio Settings")
        self.btn_save = QPushButton("Save Config")
        self.btn_save.clicked.connect(self.save_config)
        self.btn_settings.clicked.connect(self.open_settings)
        self.btn_close = QPushButton("Close")
        self.btn_close.clicked.connect(self.close)
        
        self.status_label = QLabel("No configuration loaded.")
        
        action_layout.addWidget(self.btn_settings)
        action_layout.addStretch()
        action_layout.addWidget(self.btn_load)
        action_layout.addWidget(self.btn_save)
        action_layout.addWidget(self.btn_close)
        action_layout.addStretch()
        action_layout.addWidget(self.status_label)

        main_layout.addLayout(action_layout)
        self.setCentralWidget(main_widget)

    def closeEvent(self, event):
        self.settings_manager.settings["window_geometry"] = self.saveGeometry().toHex().data().decode('utf-8')
        self.settings_manager.save()
        super().closeEvent(event)

    def apply_settings(self):
        font_family = self.settings_manager.settings.get("font_family", "")
        font_size = self.settings_manager.settings.get("font_size", 10)
        font = QFont(font_family, font_size) if font_family else QFont()
        if not font_family:
            font.setPointSize(font_size)
        QApplication.instance().setFont(font)
        
        theme_name = self.settings_manager.settings.get("theme", "VS Code Dark+")
        qss = config_studio_themes.THEMES.get(theme_name, "")
        
        # Add HelpPopup specific styles dependent on theme lightness
        if 'Dark' in theme_name or 'Monokai' in theme_name:
            qss += "\n#HelpPopup { background-color: #333333; color: #FFFFFF; border: 1px solid #555555; padding: 10px; }"
        else:
            qss += "\n#HelpPopup { background-color: #F0F0F0; color: #000000; border: 1px solid #CCCCCC; padding: 10px; }"
            
        QApplication.instance().setStyleSheet(qss)

    def open_settings(self):
        dlg = SettingsDialog(self.settings_manager, self)
        dlg.exec()

    def show_help_popup(self, text):
        self.popup = HelpPopup(text, self)
        self.popup.move(QCursor.pos())
        self.popup.show()

    def add_help_row(self, layout, key, label_text, widget):
        help_data = config_help.HELP_TEXTS.get(key, {'short': '', 'full': ''})
        
        is_checkbox = isinstance(widget, QCheckBox) and label_text == ""
        
        h_layout = QHBoxLayout()
        h_layout.setContentsMargins(0, 0, 0, 0)
        
        lbl = None
        if not is_checkbox:
            lbl = QLabel(label_text)
            if help_data['short']:
                lbl.setToolTip(help_data['short'])
            h_layout.addWidget(lbl)
            
        if help_data['short'] and hasattr(widget, 'setToolTip'):
            widget.setToolTip(help_data['short'])
            
        if isinstance(widget, QSpinBox):
            widget.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
            
        if isinstance(widget, QComboBox):
            widget.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
            widget.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
            
        if is_checkbox:
            h_layout.addWidget(widget)
            
        btn = QPushButton("i")
        btn.setObjectName("HelpButton")
        btn.setFixedSize(20, 20)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        if help_data['full']:
            btn.clicked.connect(lambda _, txt=help_data['full']: self.show_help_popup(txt))
        else:
            btn.setVisible(False)
            
        h_layout.addWidget(btn)
        h_layout.addStretch()
        
        if isinstance(layout, QFormLayout):
            if is_checkbox:
                layout.addRow("", h_layout)
            else:
                label_container = QWidget()
                label_container.setLayout(h_layout)
                layout.addRow(label_container, widget)
        else:
            row_layout = QHBoxLayout()
            if not is_checkbox:
                row_layout.addLayout(h_layout)
                row_layout.addWidget(widget)
            else:
                row_layout.addLayout(h_layout)
            layout.addLayout(row_layout)
        
        return lbl

    def on_connectivity_changed(self, text):
        self.j_driver.setEnabled(text == 'jdbc')
        self.j_libs.setEnabled(text == 'jdbc')
        self.lbl_j_driver.setEnabled(text == 'jdbc')
        self.lbl_j_libs.setEnabled(text == 'jdbc')
        
        self.o_driver.setEnabled(text == 'odbc')
        self.o_libs.setEnabled(text == 'odbc')
        self.lbl_o_driver.setEnabled(text == 'odbc')
        self.lbl_o_libs.setEnabled(text == 'odbc')
        
        self.d_path.setEnabled(text == 'ddl')
        self.lbl_d_path.setEnabled(text == 'ddl')

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
        self.add_help_row(mig_layout, 'mig_host', "Host:", self.mig_host)
        self.add_help_row(mig_layout, 'mig_port', "Port:", self.mig_port)
        self.add_help_row(mig_layout, 'mig_db', "Database:", self.mig_db)
        self.add_help_row(mig_layout, 'mig_schema', "Schema:", self.mig_schema)
        self.add_help_row(mig_layout, 'mig_user', "Username:", self.mig_user)
        self.add_help_row(mig_layout, 'mig_pass', "Password:", self.mig_pass)

        # Source Connection
        source_group = QGroupBox("Source Database")
        s_layout = QFormLayout(source_group)
        self.s_type = QComboBox()
        self.s_type.addItems(["informix", "sybase_ase", "mssql", "ibm_db2_luw", "ibm_db2_zos", "mysql", "sql_anywhere", "postgresql", "oracle"])
        self.s_type.currentTextChanged.connect(self.update_system_catalog_items)
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
        self.s_system_catalog = QComboBox()
        self.update_system_catalog_items(self.s_type.currentText())
        self.s_db_locale = QLineEdit()
        self.s_conn_opts = QLineEdit()
        
        s_btn_test = QPushButton("Test Source Connection")
        s_btn_test.clicked.connect(lambda: self.test_connection('source'))

        self.add_help_row(s_layout, 's_type', "Database Type:", self.s_type)
        self.add_help_row(s_layout, 's_conn', "Connectivity:", self.s_conn)
        self.add_help_row(s_layout, 's_host', "Host:", self.s_host)
        self.add_help_row(s_layout, 's_port', "Port:", self.s_port)
        self.add_help_row(s_layout, 's_db', "Database/Service:", self.s_db)
        self.add_help_row(s_layout, 's_schema', "Schema:", self.s_schema)
        self.add_help_row(s_layout, 's_user', "Username:", self.s_user)
        self.add_help_row(s_layout, 's_pass', "Password:", self.s_pass)
        self.add_help_row(s_layout, 's_conn_opts', "Connection String Options:", self.s_conn_opts)
        self.add_help_row(s_layout, 's_system_catalog', "System Catalog:", self.s_system_catalog)
        self.add_help_row(s_layout, 's_db_locale', "DB Locale:", self.s_db_locale)
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
        
        self.t_work_mem = QLineEdit()
        self.t_maint_work_mem = QLineEdit()
        self.t_role = QLineEdit()
        self.t_search_path = QLineEdit()

        t_btn_test = QPushButton("Test Target Connection")
        t_btn_test.clicked.connect(lambda: self.test_connection('target'))

        self.add_help_row(t_layout, 't_type', "Database Type:", self.t_type)
        self.add_help_row(t_layout, 't_host', "Host:", self.t_host)
        self.add_help_row(t_layout, 't_port', "Port:", self.t_port)
        self.add_help_row(t_layout, 't_db', "Database:", self.t_db)
        self.add_help_row(t_layout, 't_schema', "Schema:", self.t_schema)
        self.add_help_row(t_layout, 't_user', "Username:", self.t_user)
        self.add_help_row(t_layout, 't_pass', "Password:", self.t_pass)
        self.add_help_row(t_layout, 't_work_mem', "Work Mem:", self.t_work_mem)
        self.add_help_row(t_layout, 't_maint_work_mem', "Maintenance Work Mem:", self.t_maint_work_mem)
        self.add_help_row(t_layout, 't_role', "Role:", self.t_role)
        self.add_help_row(t_layout, 't_search_path', "Search Path:", self.t_search_path)
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
        drv_group = QGroupBox("Source Database Driver Libraries")
        d_layout = QFormLayout(drv_group)
        
        self.s_conn_env = QComboBox()
        self.s_conn_env.addItems(["native", "jdbc", "odbc", "ddl"])
        
        self.j_driver = QLineEdit()
        self.j_libs = QLineEdit()
        self.o_driver = QLineEdit()
        self.o_libs = QLineEdit()
        self.d_path = QLineEdit()

        self.add_help_row(d_layout, 's_conn', "Connectivity:", self.s_conn_env)
        self.lbl_j_driver = self.add_help_row(d_layout, 'j_driver', "JDBC Driver:", self.j_driver)
        self.lbl_j_libs = self.add_help_row(d_layout, 'j_libs', "JDBC Libraries:", self.j_libs)
        self.lbl_o_driver = self.add_help_row(d_layout, 'o_driver', "ODBC Driver:", self.o_driver)
        self.lbl_o_libs = self.add_help_row(d_layout, 'o_libs', "ODBC Libraries:", self.o_libs)
        self.lbl_d_path = self.add_help_row(d_layout, 'd_path', "DDL Path:", self.d_path)

        # Environment Variables
        env_group = QGroupBox("Environment Variables (format: NAME=VALUE, one per line)")
        env_layout = QVBoxLayout(env_group)
        self.env_vars = QPlainTextEdit()
        self.add_help_row(env_layout, 'env_vars', "", self.env_vars)

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
        
        self.m_on_error = QComboBox()
        self.m_on_error.addItems(["continue", "stop"])
        self.m_names_case = QComboBox()
        self.m_names_case.addItems(["lower", "upper", "keep"])

        self.m_batch_size = QSpinBox()
        self.m_batch_size.setRange(1, 10000000)
        self.m_chunk_size = QSpinBox()
        self.m_chunk_size.setRange(-1, 10000000)
        self.m_chunk_size.setSpecialValueText("Disabled (-1)")
        self.m_workers = QSpinBox()
        self.m_workers.setRange(1, 128)
        
        self.m_varchar_len = QSpinBox()
        self.m_varchar_len.setRange(-1, 1000000)
        self.m_varchar_len.setSpecialValueText("Never convert (-1)")
        self.m_char_len = QSpinBox()
        self.m_char_len.setRange(-1, 1000000)
        self.m_char_len.setSpecialValueText("Never convert (-1)")

        self.add_help_row(m_layout, 'm_workflow', "Workflow:", self.m_workflow)
        self.add_help_row(m_layout, 'm_on_error', "On Error:", self.m_on_error)
        self.add_help_row(m_layout, 'm_names_case', "Names Case Handling:", self.m_names_case)
        self.add_help_row(m_layout, 'm_batch_size', "Batch Size:", self.m_batch_size)
        self.add_help_row(m_layout, 'm_chunk_size', "Chunk Size:", self.m_chunk_size)
        self.add_help_row(m_layout, 'm_workers', "Parallel Workers:", self.m_workers)
        self.add_help_row(m_layout, 'm_varchar_len', "Varchar to Text Length:", self.m_varchar_len)
        self.add_help_row(m_layout, 'm_char_len', "Char to Text Length:", self.m_char_len)

        actions_group = QGroupBox("Migration Phases")
        a_layout = QVBoxLayout(actions_group)
        self.m_drop_schema = QCheckBox("Drop schema if exists")
        self.m_recreate_schema = QCheckBox("Recreate schema")
        self.m_drop_tables = QCheckBox("Drop tables")
        self.m_truncate_tables = QCheckBox("Truncate tables")
        self.m_create_tables = QCheckBox("Create tables")
        self.m_migrate_data = QCheckBox("Migrate data")
        self.m_migrate_indexes = QCheckBox("Migrate indexes")
        self.m_migrate_constraints = QCheckBox("Migrate constraints")
        self.m_migrate_funcprocs = QCheckBox("Migrate functions/procedures")
        self.m_migrate_triggers = QCheckBox("Migrate triggers")
        self.m_migrate_views = QCheckBox("Migrate views")
        self.m_set_sequences = QCheckBox("Set sequences")
        self.m_use_aliases = QCheckBox("Use aliases as target names")
        self.m_migrate_lob = QCheckBox("Migrate LOB values")
        
        self.add_help_row(a_layout, 'm_drop_schema', "", self.m_drop_schema)
        self.add_help_row(a_layout, 'm_recreate_schema', "", self.m_recreate_schema)
        self.add_help_row(a_layout, 'm_drop_tables', "", self.m_drop_tables)
        self.add_help_row(a_layout, 'm_truncate_tables', "", self.m_truncate_tables)
        self.add_help_row(a_layout, 'm_create_tables', "", self.m_create_tables)
        self.add_help_row(a_layout, 'm_migrate_data', "", self.m_migrate_data)
        self.add_help_row(a_layout, 'm_migrate_indexes', "", self.m_migrate_indexes)
        self.add_help_row(a_layout, 'm_migrate_constraints', "", self.m_migrate_constraints)
        self.add_help_row(a_layout, 'm_migrate_funcprocs', "", self.m_migrate_funcprocs)
        self.add_help_row(a_layout, 'm_migrate_triggers', "", self.m_migrate_triggers)
        self.add_help_row(a_layout, 'm_migrate_views', "", self.m_migrate_views)
        self.add_help_row(a_layout, 'm_set_sequences', "", self.m_set_sequences)
        self.add_help_row(a_layout, 'm_use_aliases', "", self.m_use_aliases)
        self.add_help_row(a_layout, 'm_migrate_lob', "", self.m_migrate_lob)

        layout.addWidget(mig_group)
        layout.addWidget(actions_group)
        layout.addStretch()
        return tab

    def create_validator_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        val_group = QGroupBox("Validator")
        v_layout = QFormLayout(val_group)
        
        self.use_validator = QCheckBox("Enable Configuration")
        self.use_validator.setStyleSheet("font-weight: bold;")
        self.use_validator.toggled.connect(self.toggle_validator)
        self.add_help_row(v_layout, 'use_validator', "Use Validator:", self.use_validator)
        
        self.v_row_counts = QCheckBox("Check Row Counts")
        self.v_table_checksums = QCheckBox("Check Table Checksums")
        self.v_random_sample = QCheckBox("Check Random Sample")
        self.v_lob_sizes = QCheckBox("Check LOB Sizes")
        self.v_sample_size = QSpinBox()
        self.v_sample_size.setRange(1, 100000)
        self.v_workers = QSpinBox()
        self.v_workers.setRange(1, 128)

        self.add_help_row(v_layout, 'v_row_counts', "", self.v_row_counts)
        self.add_help_row(v_layout, 'v_table_checksums', "", self.v_table_checksums)
        self.add_help_row(v_layout, 'v_random_sample', "", self.v_random_sample)
        self.add_help_row(v_layout, 'v_lob_sizes', "", self.v_lob_sizes)
        self.add_help_row(v_layout, 'v_workers', "Validator Workers:", self.v_workers)
        self.add_help_row(v_layout, 'v_sample_size', "Random Sample Size:", self.v_sample_size)

        layout.addWidget(val_group)
        layout.addStretch()
        return tab

    def create_premigration_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        info_label = QLabel("Configure Pre-Migration Analysis (Source Database):")
        info_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(info_label)
        
        pma_group = QGroupBox("Top N Tables Listing")
        pma_layout = QFormLayout(pma_group)
        
        self.use_premigration = QCheckBox("Enable Configuration")
        self.use_premigration.setStyleSheet("font-weight: bold;")
        self.use_premigration.toggled.connect(self.toggle_premigration)
        self.add_help_row(pma_layout, 'use_premigration', "Use Analysis:", self.use_premigration)
        
        self.pma_by_rows = QSpinBox()
        self.pma_by_rows.setRange(0, 9999999)
        self.pma_by_size = QSpinBox()
        self.pma_by_size.setRange(0, 9999999)
        self.pma_by_columns = QSpinBox()
        self.pma_by_columns.setRange(0, 9999999)
        self.pma_by_indexes = QSpinBox()
        self.pma_by_indexes.setRange(0, 9999999)
        self.pma_by_constraints = QSpinBox()
        self.pma_by_constraints.setRange(0, 9999999)
        
        self.add_help_row(pma_layout, 'pma_by_rows', "By Rows:", self.pma_by_rows)
        self.add_help_row(pma_layout, 'pma_by_size', "By Size:", self.pma_by_size)
        self.add_help_row(pma_layout, 'pma_by_columns', "By Columns:", self.pma_by_columns)
        self.add_help_row(pma_layout, 'pma_by_indexes', "By Indexes:", self.pma_by_indexes)
        self.add_help_row(pma_layout, 'pma_by_constraints', "By Constraints:", self.pma_by_constraints)
        
        layout.addWidget(pma_group)
        layout.addStretch()
        return tab

    def create_data_export_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        info_label = QLabel("Configure Source Data Export Files Migration (Informix Only):")
        info_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(info_label)

        de_group = QGroupBox("Data Export File Settings")
        de_layout = QFormLayout(de_group)
        
        self.de_use_data_export = QCheckBox("Enable Configuration")
        self.de_use_data_export.setStyleSheet("font-weight: bold;")
        self.de_use_data_export.toggled.connect(self.toggle_data_export)
        self.add_help_row(de_layout, 'de_use_data_export', "Use Data Export:", self.de_use_data_export)

        self.de_on_missing = QComboBox()
        self.de_on_missing.addItems(["error", "skip", "source_table_name"])
        self.de_format = QComboBox()
        self.de_format.addItems(["CSV", "UNL", "SQL"])
        self.de_file = QLineEdit()
        self.de_delimiter = QLineEdit()
        self.de_header = QCheckBox("File contains header")
        self.de_charset = QLineEdit()
        self.de_conv_path = QLineEdit()
        self.de_clean = QCheckBox("Clean up converted files")

        self.add_help_row(de_layout, 'de_on_missing', "On Missing File:", self.de_on_missing)
        self.add_help_row(de_layout, 'de_format', "Format:", self.de_format)
        self.add_help_row(de_layout, 'de_file', "File Pattern:", self.de_file)
        self.add_help_row(de_layout, 'de_delimiter', "Delimiter:", self.de_delimiter)
        self.add_help_row(de_layout, 'de_header', "Header:", self.de_header)
        self.add_help_row(de_layout, 'de_charset', "Character Set:", self.de_charset)
        self.add_help_row(de_layout, 'de_conv_path', "Conversion Path:", self.de_conv_path)
        self.add_help_row(de_layout, 'de_clean', "Clean Up:", self.de_clean)
        
        layout.addWidget(de_group)
        
        split_group = QGroupBox("Big Files Split")
        split_layout = QFormLayout(split_group)
        
        self.de_use_split = QCheckBox("Enable Split")
        self.de_use_split.setStyleSheet("font-weight: bold;")
        self.de_use_split.toggled.connect(self.toggle_split)
        self.add_help_row(split_layout, 'de_use_split', "Use Split:", self.de_use_split)

        self.de_threshold = QLineEdit()
        self.de_chunk = QLineEdit()
        self.de_workers = QSpinBox()
        self.de_workers.setRange(1, 128)
        
        self.add_help_row(split_layout, 'de_threshold', "Threshold:", self.de_threshold)
        self.add_help_row(split_layout, 'de_chunk', "Chunk Size:", self.de_chunk)
        self.add_help_row(split_layout, 'de_workers', "Workers:", self.de_workers)

        layout.addWidget(split_group)
        layout.addStretch()
        return tab

    def create_table_filters_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        row1 = QHBoxLayout()
        inc_group = QGroupBox("Include Tables")
        inc_layout = QVBoxLayout(inc_group)
        self.inc_tables = QPlainTextEdit()
        self.add_help_row(inc_layout, 'inc_tables', "", self.inc_tables)

        exc_group = QGroupBox("Exclude Tables")
        exc_layout = QVBoxLayout(exc_group)
        self.exc_tables = QPlainTextEdit()
        self.add_help_row(exc_layout, 'exc_tables', "", self.exc_tables)
        row1.addWidget(inc_group)
        row1.addWidget(exc_group)

        row2 = QHBoxLayout()
        inc_v_group = QGroupBox("Include Views")
        inc_v_layout = QVBoxLayout(inc_v_group)
        self.inc_views = QPlainTextEdit()
        self.add_help_row(inc_v_layout, 'inc_views', "", self.inc_views)

        exc_v_group = QGroupBox("Exclude Views")
        exc_v_layout = QVBoxLayout(exc_v_group)
        self.exc_views = QPlainTextEdit()
        self.add_help_row(exc_v_layout, 'exc_views', "", self.exc_views)
        row2.addWidget(inc_v_group)
        row2.addWidget(exc_v_group)

        row3 = QHBoxLayout()
        inc_f_group = QGroupBox("Include Func/Procs")
        inc_f_layout = QVBoxLayout(inc_f_group)
        self.inc_funcprocs = QPlainTextEdit()
        self.add_help_row(inc_f_layout, 'inc_funcprocs', "", self.inc_funcprocs)

        exc_f_group = QGroupBox("Exclude Func/Procs")
        exc_f_layout = QVBoxLayout(exc_f_group)
        self.exc_funcprocs = QPlainTextEdit()
        self.add_help_row(exc_f_layout, 'exc_funcprocs', "", self.exc_funcprocs)
        row3.addWidget(inc_f_group)
        row3.addWidget(exc_f_group)

        layout.addLayout(row1)
        layout.addLayout(row2)
        layout.addLayout(row3)
        return tab

    def create_advanced_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        adv_group = QGroupBox("Advanced YAML Editor")
        adv_layout = QVBoxLayout(adv_group)
        self.adv_yaml = QPlainTextEdit()
        
        font = self.adv_yaml.font()
        font.setFamily("Monospace")
        self.adv_yaml.setFont(font)
        
        self.add_help_row(adv_layout, 'adv_yaml', "", self.adv_yaml)

        layout.addWidget(adv_group)
        return tab

    def toggle_validator(self, checked):
        self.v_row_counts.setEnabled(checked)
        self.v_table_checksums.setEnabled(checked)
        self.v_random_sample.setEnabled(checked)
        self.v_lob_sizes.setEnabled(checked)
        self.v_sample_size.setEnabled(checked)
        self.v_workers.setEnabled(checked)

    def update_system_catalog_items(self, db_type):
        current = self.s_system_catalog.currentText()
        self.s_system_catalog.blockSignals(True)
        self.s_system_catalog.clear()
        
        if db_type == "ibm_db2_luw":
            items = ["SYSCAT", "SYSIBM"]
        elif db_type == "mssql":
            items = ["SYS", "INFORMATION_SCHEMA"]
        else:
            items = [""]
            
        self.s_system_catalog.addItems(items)
        if current in items:
            self.s_system_catalog.setCurrentText(current)
        self.s_system_catalog.blockSignals(False)

    def toggle_premigration(self, checked):
        self.pma_by_rows.setEnabled(checked)
        self.pma_by_size.setEnabled(checked)
        self.pma_by_columns.setEnabled(checked)
        self.pma_by_indexes.setEnabled(checked)
        self.pma_by_constraints.setEnabled(checked)

    def toggle_data_export(self, checked):
        self.de_on_missing.setEnabled(checked)
        self.de_format.setEnabled(checked)
        self.de_file.setEnabled(checked)
        self.de_delimiter.setEnabled(checked)
        self.de_header.setEnabled(checked)
        self.de_charset.setEnabled(checked)
        self.de_conv_path.setEnabled(checked)
        self.de_clean.setEnabled(checked)
        self.de_use_split.setEnabled(checked)
        self.toggle_split(checked and self.de_use_split.isChecked())

    def toggle_split(self, checked):
        parent_checked = self.de_use_data_export.isChecked()
        state = checked and parent_checked
        self.de_threshold.setEnabled(state)
        self.de_chunk.setEnabled(state)
        self.de_workers.setEnabled(state)

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
        self.s_conn_opts.setText(src.get('connection_string_options', ''))
        self.s_system_catalog.setCurrentText(src.get('system_catalog', ''))
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
        
        tgt_settings = tgt.get('settings', {})
        self.t_work_mem.setText(tgt_settings.get('work_mem', ''))
        self.t_maint_work_mem.setText(tgt_settings.get('maintenance_work_mem', ''))
        self.t_role.setText(tgt_settings.get('role', ''))
        self.t_search_path.setText(tgt_settings.get('search_path', ''))

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

        # Data Export
        has_de = 'data_export' in src
        self.de_use_data_export.setChecked(has_de)
        de = src.get('data_export', {})
        self.de_on_missing.setCurrentText(de.get('on_missing_data_file', 'source_table_name'))
        self.de_format.setCurrentText(de.get('format', 'CSV'))
        self.de_file.setText(de.get('file', ''))
        self.de_delimiter.setText(de.get('delimiter', ','))
        self.de_header.setChecked(de.get('header', False))
        self.de_charset.setText(de.get('character_set', 'UTF-8'))
        self.de_conv_path.setText(de.get('conversion_path', ''))
        self.de_clean.setChecked(de.get('clean', False))

        bf_split = de.get('big_files_split', {})
        has_split = bf_split.get('enabled', False)
        self.de_use_split.setChecked(has_split)
        self.de_threshold.setText(bf_split.get('threshold', '5GB'))
        self.de_chunk.setText(bf_split.get('chunk_size', '2GB'))
        self.de_workers.setValue(bf_split.get('workers', 4))
        
        self.toggle_data_export(has_de)

        # Migration Settings
        mig = self.config_data.get('migration', {})
        self.m_workflow.setCurrentText(mig.get('workflow', 'standard'))
        self.m_on_error.setCurrentText(mig.get('on_error', 'continue'))
        self.m_names_case.setCurrentText(mig.get('names_case_handling', 'lower'))
        self.m_batch_size.setValue(mig.get('batch_size', 100000))
        self.m_chunk_size.setValue(mig.get('chunk_size', -1))
        self.m_workers.setValue(mig.get('parallel_workers', 4))
        self.m_varchar_len.setValue(mig.get('varchar_to_text_length', -1))
        self.m_char_len.setValue(mig.get('char_to_text_length', -1))
        
        self.m_drop_schema.setChecked(mig.get('drop_schema', False))
        self.m_recreate_schema.setChecked(mig.get('recreate_schema', False))
        self.m_drop_tables.setChecked(mig.get('drop_tables', True))
        self.m_truncate_tables.setChecked(mig.get('truncate_tables', True))
        self.m_create_tables.setChecked(mig.get('create_tables', True))
        self.m_migrate_data.setChecked(mig.get('migrate_data', True))
        self.m_migrate_indexes.setChecked(mig.get('migrate_indexes', True))
        self.m_migrate_constraints.setChecked(mig.get('migrate_constraints', True))
        self.m_migrate_funcprocs.setChecked(mig.get('migrate_funcprocs', True))
        self.m_migrate_triggers.setChecked(mig.get('migrate_triggers', True))
        self.m_migrate_views.setChecked(mig.get('migrate_views', True))
        self.m_set_sequences.setChecked(mig.get('set_sequences', True))
        self.m_use_aliases.setChecked(mig.get('use_aliases_as_target_names', False))
        self.m_migrate_lob.setChecked(mig.get('migrate_lob_values', True))

        # Validator
        has_val = 'validator' in self.config_data
        self.use_validator.setChecked(has_val)
        self.toggle_validator(has_val)
        val = self.config_data.get('validator', {})
        self.v_row_counts.setChecked(val.get('check_row_counts', False))
        self.v_table_checksums.setChecked(val.get('check_table_checksums', False))
        self.v_random_sample.setChecked(val.get('check_random_sample', False))
        self.v_lob_sizes.setChecked(val.get('check_lob_sizes', False))
        self.v_workers.setValue(val.get('workers', 4))
        self.v_sample_size.setValue(val.get('random_sample_size', 1000))

        # Premigration Analysis
        has_pma = 'pre_migration_analysis' in self.config_data
        self.use_premigration.setChecked(has_pma)
        self.toggle_premigration(has_pma)
        pma = self.config_data.get('pre_migration_analysis', {}).get('top_n_tables', {})
        self.pma_by_rows.setValue(pma.get('by_rows', 10))
        self.pma_by_size.setValue(pma.get('by_size', 10))
        self.pma_by_columns.setValue(pma.get('by_columns', 10))
        self.pma_by_indexes.setValue(pma.get('by_indexes', 10))
        self.pma_by_constraints.setValue(pma.get('by_constraints', 10))

        # Include/Exclude Tables
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

        # Include/Exclude Views
        inc_v = self.config_data.get('include_views', [])
        if inc_v is None: inc_v = []
        if isinstance(inc_v, str):
            self.inc_views.setPlainText(inc_v)
        else:
            self.inc_views.setPlainText("\n".join(inc_v))

        exc_v = self.config_data.get('exclude_views', [])
        if exc_v is None: exc_v = []
        if isinstance(exc_v, str):
            self.exc_views.setPlainText(exc_v)
        else:
            self.exc_views.setPlainText("\n".join(exc_v))

        # Include/Exclude Func/Procs
        inc_f = self.config_data.get('include_funcprocs', [])
        if inc_f is None: inc_f = []
        if isinstance(inc_f, str):
            self.inc_funcprocs.setPlainText(inc_f)
        else:
            self.inc_funcprocs.setPlainText("\n".join(inc_f))

        exc_f = self.config_data.get('exclude_funcprocs', [])
        if exc_f is None: exc_f = []
        if isinstance(exc_f, str):
            self.exc_funcprocs.setPlainText(exc_f)
        else:
            self.exc_funcprocs.setPlainText("\n".join(exc_f))

        # Advanced YAML Editor
        adv_dict = {}
        for key in ['table_settings', 'mapping_workflow', 'anonymization', 'scheduled_actions', 'data_types_substitution', 'default_values_substitution', 'remote_objects_substitution', 'data_migration_limitation', 'summary']:
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
        for block in ['migrator', 'source', 'target', 'migration']:
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
        if self.s_conn_opts.text(): src['connection_string_options'] = self.s_conn_opts.text()
        if self.s_system_catalog.currentText(): src['system_catalog'] = self.s_system_catalog.currentText()
        if self.s_db_locale.text(): src['db_locale'] = self.s_db_locale.text()

        tgt = self.config_data['target']
        tgt['type'] = self.t_type.currentText()
        tgt['host'] = self.t_host.text()
        tgt['port'] = self.t_port.value()
        tgt['database'] = self.t_db.text()
        tgt['schema'] = self.t_schema.text()
        tgt['username'] = self.t_user.text()
        tgt['password'] = self.t_pass.text()
        
        tgt_settings = {}
        if self.t_work_mem.text(): tgt_settings['work_mem'] = self.t_work_mem.text()
        if self.t_maint_work_mem.text(): tgt_settings['maintenance_work_mem'] = self.t_maint_work_mem.text()
        if self.t_role.text(): tgt_settings['role'] = self.t_role.text()
        if self.t_search_path.text(): tgt_settings['search_path'] = self.t_search_path.text()
        if tgt_settings: tgt['settings'] = tgt_settings

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

        if self.de_use_data_export.isChecked():
            de = {
                'on_missing_data_file': self.de_on_missing.currentText(),
                'format': self.de_format.currentText(),
            }
            if self.de_file.text(): de['file'] = self.de_file.text()
            if self.de_delimiter.text(): de['delimiter'] = self.de_delimiter.text()
            de['header'] = self.de_header.isChecked()
            if self.de_charset.text(): de['character_set'] = self.de_charset.text()
            if self.de_conv_path.text(): de['conversion_path'] = self.de_conv_path.text()
            de['clean'] = self.de_clean.isChecked()

            if self.de_use_split.isChecked():
                de['big_files_split'] = {
                    'enabled': True,
                    'threshold': self.de_threshold.text(),
                    'chunk_size': self.de_chunk.text(),
                    'workers': self.de_workers.value()
                }
            src['data_export'] = de
        else:
            if 'data_export' in src:
                del src['data_export']

        mig = self.config_data['migration']
        mig['workflow'] = self.m_workflow.currentText()
        mig['on_error'] = self.m_on_error.currentText()
        mig['names_case_handling'] = self.m_names_case.currentText()
        mig['batch_size'] = self.m_batch_size.value()
        mig['chunk_size'] = self.m_chunk_size.value()
        mig['parallel_workers'] = self.m_workers.value()
        if self.m_varchar_len.value() != -1:
            mig['varchar_to_text_length'] = self.m_varchar_len.value()
        if self.m_char_len.value() != -1:
            mig['char_to_text_length'] = self.m_char_len.value()
            
        mig['drop_schema'] = self.m_drop_schema.isChecked()
        mig['recreate_schema'] = self.m_recreate_schema.isChecked()
        mig['drop_tables'] = self.m_drop_tables.isChecked()
        mig['truncate_tables'] = self.m_truncate_tables.isChecked()
        mig['create_tables'] = self.m_create_tables.isChecked()
        mig['migrate_data'] = self.m_migrate_data.isChecked()
        mig['migrate_indexes'] = self.m_migrate_indexes.isChecked()
        mig['migrate_constraints'] = self.m_migrate_constraints.isChecked()
        mig['migrate_funcprocs'] = self.m_migrate_funcprocs.isChecked()
        mig['migrate_triggers'] = self.m_migrate_triggers.isChecked()
        mig['migrate_views'] = self.m_migrate_views.isChecked()
        mig['set_sequences'] = self.m_set_sequences.isChecked()
        mig['use_aliases_as_target_names'] = self.m_use_aliases.isChecked()
        mig['migrate_lob_values'] = self.m_migrate_lob.isChecked()

        if self.use_validator.isChecked():
            val = self.config_data.get('validator', {})
            val['check_row_counts'] = self.v_row_counts.isChecked()
            val['check_table_checksums'] = self.v_table_checksums.isChecked()
            val['check_random_sample'] = self.v_random_sample.isChecked()
            val['check_lob_sizes'] = self.v_lob_sizes.isChecked()
            val['workers'] = self.v_workers.value()
            val['random_sample_size'] = self.v_sample_size.value()
            self.config_data['validator'] = val
        else:
            if 'validator' in self.config_data:
                del self.config_data['validator']

        # Premigration Analysis
        if self.use_premigration.isChecked():
            pma_block = {
                'by_rows': self.pma_by_rows.value(),
                'by_size': self.pma_by_size.value(),
                'by_columns': self.pma_by_columns.value(),
                'by_indexes': self.pma_by_indexes.value(),
                'by_constraints': self.pma_by_constraints.value(),
            }
            self.config_data['pre_migration_analysis'] = {'top_n_tables': pma_block}
        else:
            if 'pre_migration_analysis' in self.config_data:
                del self.config_data['pre_migration_analysis']

        # Filters
        inc_txt = self.inc_tables.toPlainText().strip()
        if inc_txt == 'all':
            self.config_data['include_tables'] = 'all'
        elif inc_txt:
            self.config_data['include_tables'] = [l.strip() for l in inc_txt.split('\n') if l.strip()]

        exc_txt = self.exc_tables.toPlainText().strip()
        if exc_txt:
            self.config_data['exclude_tables'] = [l.strip() for l in exc_txt.split('\n') if l.strip()]

        inc_v_txt = self.inc_views.toPlainText().strip()
        if inc_v_txt == 'all':
            self.config_data['include_views'] = 'all'
        elif inc_v_txt:
            self.config_data['include_views'] = [l.strip() for l in inc_v_txt.split('\n') if l.strip()]

        exc_v_txt = self.exc_views.toPlainText().strip()
        if exc_v_txt:
            self.config_data['exclude_views'] = [l.strip() for l in exc_v_txt.split('\n') if l.strip()]

        inc_f_txt = self.inc_funcprocs.toPlainText().strip()
        if inc_f_txt == 'all':
            self.config_data['include_funcprocs'] = 'all'
        elif inc_f_txt:
            self.config_data['include_funcprocs'] = [l.strip() for l in inc_f_txt.split('\n') if l.strip()]

        exc_f_txt = self.exc_funcprocs.toPlainText().strip()
        if exc_f_txt:
            self.config_data['exclude_funcprocs'] = [l.strip() for l in exc_f_txt.split('\n') if l.strip()]

        # Advanced
        adv_txt = self.adv_yaml.toPlainText().strip()
        if adv_txt:
            try:
                parsed_adv = self.yaml.load(adv_txt)
                if parsed_adv and isinstance(parsed_adv, dict):
                    for k, v in parsed_adv.items():
                        if k == 'source:data_export':
                            self.config_data['source']['data_export'] = v
                        else:
                            self.config_data[k] = v
            except Exception as e:
                raise Exception(f"Failed to parse Advanced YAML section: {str(e)}")

    def validate_configuration(self):
        # Validate chunk_size vs batch_size
        mig = self.config_data.get('migration', {})
        chunk_size = mig.get('chunk_size', -1)
        batch_size = mig.get('batch_size', 100000)
        if chunk_size != -1 and chunk_size <= batch_size:
            raise Exception("Contradictory settings: migration 'chunk_size' must be greater than 'batch_size', or set to -1 to disable chunking.")
            
        # Validate schema vs owner
        src = self.config_data.get('source', {})
        if src.get('schema') and src.get('owner'):
            raise Exception("Contradictory settings: Source configuration cannot define both 'schema' and 'owner'. They are synonyms, use only one.")

    def save_config(self):
        if not self.current_file:
            self.current_file, _ = QFileDialog.getSaveFileName(self, "Save Config File", "", "YAML Files (*.yaml *.yml);;All Files (*)")
            if not self.current_file: return

        try:
            self.update_data_from_ui()
            self.validate_configuration()
            
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
            if isinstance(e, ImportError):
                QMessageBox.critical(self, "Driver Missing", f"Could not load the database driver for '{db_type}'.\nError: {str(e)}\n\nPlease ensure you have activated your virtual environment (e.g., 'source venv/bin/activate') or installed the required packages.")
            else:
                QMessageBox.critical(self, "Connection Failed", f"Could not connect to the {db_side} database:\n{str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet("QPushButton { background-color: blue; color: white; }")
    window = ConfigStudio()
    window.show()
    sys.exit(app.exec())
