THEMES = {
    "VS Code Dark+": """
        QWidget {
            background-color: #1E1E1E;
            color: #D4D4D4;
        }
        QGroupBox {
            border: 1px solid #3C3C3C;
            margin-top: 1.5ex;
            padding-top: 10px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 0 3px;
            color: #569CD6;
        }
        QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {
            background-color: #3C3C3C;
            border: 1px solid #3C3C3C;
            color: #D4D4D4;
            padding: 2px;
        }
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {
            border: 1px solid #007ACC;
        }
        QLabel:disabled, QCheckBox:disabled {
            color: #666666;
        }
        QLineEdit:disabled, QSpinBox:disabled, QComboBox:disabled, QPlainTextEdit:disabled {
            background-color: #2D2D2D;
            color: #666666;
            border: 1px solid #3C3C3C;
        }
        QPushButton {
            background-color: #0E639C;
            color: white;
            border: None;
            padding: 5px 10px;
        }
        QPushButton:hover {
            background-color: #1177BB;
        }
        QPushButton:pressed {
            background-color: #094771;
        }
        #HelpButton {
            padding: 0px;
            border-radius: 10px;
            font-weight: bold;
        }
        QTabWidget::pane {
            border: 1px solid #3C3C3C;
        }
        QTabBar::tab {
            background: #2D2D2D;
            color: #969696;
            padding: 8px 12px;
        }
        QTabBar::tab:selected {
            background: #1E1E1E;
            color: #FFFFFF;
            border-top: 2px solid #007ACC;
        }
        QToolTip {
            background-color: #252526;
            color: #CCCCCC;
            border: 1px solid #454545;
        }
    """,

    "VS Code Light+": """
        QWidget {
            background-color: #FFFFFF;
            color: #333333;
        }
        QGroupBox {
            border: 1px solid #CCCCCC;
            margin-top: 1.5ex;
            padding-top: 10px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 0 3px;
            color: #005A9E;
        }
        QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {
            background-color: #FFFFFF;
            border: 1px solid #CECECE;
            color: #333333;
            padding: 2px;
        }
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {
            border: 1px solid #007ACC;
        }
        QLabel:disabled, QCheckBox:disabled {
            color: #A0A0A0;
        }
        QLineEdit:disabled, QSpinBox:disabled, QComboBox:disabled, QPlainTextEdit:disabled {
            background-color: #F3F3F3;
            color: #A0A0A0;
            border: 1px solid #E0E0E0;
        }
        QPushButton {
            background-color: #007ACC;
            color: white;
            border: None;
            padding: 5px 10px;
        }
        QPushButton:hover {
            background-color: #0062A3;
        }
        QPushButton:pressed {
            background-color: #004A77;
        }
        #HelpButton {
            padding: 0px;
            border-radius: 10px;
            font-weight: bold;
        }
        QTabWidget::pane {
            border: 1px solid #CCCCCC;
        }
        QTabBar::tab {
            background: #ECECEC;
            color: #666666;
            padding: 8px 12px;
            border-right: 1px solid #CCCCCC;
        }
        QTabBar::tab:selected {
            background: #FFFFFF;
            color: #333333;
            border-top: 2px solid #007ACC;
            border-bottom: None;
        }
        QToolTip {
            background-color: #F3F3F3;
            color: #333333;
            border: 1px solid #CCCCCC;
        }
    """,

    "Monokai": """
        QWidget {
            background-color: #272822;
            color: #F8F8F2;
        }
        QGroupBox {
            border: 1px solid #3E3D32;
            margin-top: 1.5ex;
            padding-top: 10px;
        }
        QGroupBox::title {
            color: #A6E22E;
        }
        QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {
            background-color: #1E1F1C;
            border: 1px solid #3E3D32;
            color: #F8F8F2;
            padding: 2px;
        }
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {
            border: 1px solid #FD971F;
        }
        QLabel:disabled, QCheckBox:disabled {
            color: #75715E;
        }
        QLineEdit:disabled, QSpinBox:disabled, QComboBox:disabled, QPlainTextEdit:disabled {
            background-color: #272822;
            color: #75715E;
            border: 1px solid #3E3D32;
        }
        QPushButton {
            background-color: #F92672;
            color: white;
            border: None;
            padding: 5px 10px;
        }
        QPushButton:hover {
            background-color: #FF428A;
        }
        QPushButton:pressed {
            background-color: #D31B5D;
        }
        #HelpButton {
            padding: 0px;
            border-radius: 10px;
            font-weight: bold;
        }
        QTabWidget::pane {
            border: 1px solid #3E3D32;
        }
        QTabBar::tab {
            background: #1E1F1C;
            color: #75715E;
            padding: 8px 12px;
        }
        QTabBar::tab:selected {
            background: #272822;
            color: #F8F8F2;
            border-top: 2px solid #FD971F;
        }
        QToolTip {
            background-color: #3E3D32;
            color: #F8F8F2;
            border: 1px solid #75715E;
        }
    """,

    "GitHub Dark": """
        QWidget {
            background-color: #0D1117;
            color: #C9D1D9;
        }
        QGroupBox {
            border: 1px solid #30363D;
            margin-top: 1.5ex;
            padding-top: 10px;
        }
        QGroupBox::title {
            color: #58A6FF;
        }
        QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {
            background-color: #010409;
            border: 1px solid #30363D;
            color: #C9D1D9;
            padding: 2px;
        }
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {
            border: 1px solid #58A6FF;
        }
        QLabel:disabled, QCheckBox:disabled {
            color: #484F58;
        }
        QLineEdit:disabled, QSpinBox:disabled, QComboBox:disabled, QPlainTextEdit:disabled {
            background-color: #0D1117;
            color: #484F58;
            border: 1px solid #21262D;
        }
        QPushButton {
            background-color: #238636;
            color: #FFFFFF;
            border: 1px solid #2EA043;
            border-radius: 4px;
            padding: 5px 10px;
        }
        QPushButton:hover {
            background-color: #2EA043;
        }
        QPushButton:pressed {
            background-color: #1A6C2B;
        }
        #HelpButton {
            padding: 0px;
            border-radius: 10px;
            font-weight: bold;
        }
        QTabWidget::pane {
            border: 1px solid #30363D;
        }
        QTabBar::tab {
            background: #010409;
            color: #8B949E;
            padding: 8px 12px;
        }
        QTabBar::tab:selected {
            background: #0D1117;
            color: #C9D1D9;
            border-top: 2px solid #F78166;
        }
        QToolTip {
            background-color: #161B22;
            color: #8B949E;
            border: 1px solid #30363D;
        }
    """,

    "Solarized Dark": """
        QWidget {
            background-color: #002B36;
            color: #839496;
        }
        QGroupBox {
            border: 1px solid #073642;
            margin-top: 1.5ex;
            padding-top: 10px;
        }
        QGroupBox::title {
            color: #268BD2;
        }
        QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {
            background-color: #073642;
            border: 1px solid #586E75;
            color: #93A1A1;
            padding: 2px;
        }
        QLineEdit:focus, QSpinBox:focus, QComboBox:focus, QPlainTextEdit:focus {
            border: 1px solid #2AA198;
        }
        QLabel:disabled, QCheckBox:disabled {
            color: #586E75;
        }
        QLineEdit:disabled, QSpinBox:disabled, QComboBox:disabled, QPlainTextEdit:disabled {
            background-color: #002B36;
            color: #586E75;
            border: 1px solid #073642;
        }
        QPushButton {
            background-color: #2AA198;
            color: #002B36;
            border: None;
            padding: 5px 10px;
        }
        QPushButton:hover {
            background-color: #2EAFB0;
        }
        QPushButton:pressed {
            background-color: #1F7C75;
        }
        #HelpButton {
            padding: 0px;
            border-radius: 10px;
            font-weight: bold;
        }
        QTabWidget::pane {
            border: 1px solid #073642;
        }
        QTabBar::tab {
            background: #073642;
            color: #586E75;
            padding: 8px 12px;
        }
        QTabBar::tab:selected {
            background: #002B36;
            color: #93A1A1;
            border-top: 2px solid #859900;
        }
        QToolTip {
            background-color: #073642;
            color: #93A1A1;
            border: 1px solid #586E75;
        }
    """
}
