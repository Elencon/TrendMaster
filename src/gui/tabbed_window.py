"""
Tabbed main window with detachable tabs.
Allows windows to be used as tabs or dragged out as standalone windows.
"""

import logging

from PySide6.QtWidgets import (
    QMainWindow, QTabWidget, QTabBar, QWidget, QPushButton
)
from PySide6.QtCore import Qt, Signal, QPoint, QEvent

logger = logging.getLogger(__name__)


# =========================================================
# Custom Tab Bar
# =========================================================
class DetachableTabBar(QTabBar):
    """Tab bar that allows tabs to be detached into separate windows."""

    tab_detached = Signal(int, QPoint)  # tab_index, global_position

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setAcceptDrops(True)
        self.setElideMode(Qt.ElideRight)
        self.setSelectionBehaviorOnRemove(QTabBar.SelectLeftTab)
        self.setMovable(True)

        self._drag_start_pos = QPoint()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._drag_start_pos = event.pos()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if not (event.buttons() & Qt.LeftButton):
            super().mouseMoveEvent(event)
            return

        # Minimum drag threshold
        if (event.pos() - self._drag_start_pos).manhattanLength() < 30:
            super().mouseMoveEvent(event)
            return

        tab_index = self.tabAt(self._drag_start_pos)
        if tab_index < 0:
            super().mouseMoveEvent(event)
            return

        global_pos = self.mapToGlobal(event.pos())
        self.tab_detached.emit(tab_index, global_pos)

        event.accept()


# =========================================================
# Detachable Tab Widget
# =========================================================
class DetachableTabWidget(QTabWidget):
    """Tab widget with detachable tabs that become standalone windows."""

    tab_detached = Signal(QWidget, str, QPoint)

    def __init__(self, parent=None):
        super().__init__(parent)

        self._tab_bar = DetachableTabBar(self)
        self.setTabBar(self._tab_bar)

        self._tab_bar.tab_detached.connect(self._on_tab_detach_requested)

        self.setTabsClosable(True)
        self.tabCloseRequested.connect(self._on_tab_close_requested)

    def _on_tab_detach_requested(self, tab_index: int, position: QPoint):
        if not (0 <= tab_index < self.count()):
            return

        widget = self.widget(tab_index)
        title = self.tabText(tab_index)

        # Prevent widget deletion when removing tab
        widget.setParent(None)
        self.removeTab(tab_index)

        self.tab_detached.emit(widget, title, position)

    def _on_tab_close_requested(self, tab_index: int):
        widget = self.widget(tab_index)
        self.removeTab(tab_index)

        if widget and hasattr(widget, "close"):
            widget.close()


# =========================================================
# Main Window
# =========================================================
class TabbedMainWindow(QMainWindow):
    """
    Main window with tabbed interface and detachable tabs.
    Windows can be used as tabs or dragged out as standalone windows.
    """

    def __init__(self):
        super().__init__()

        self.setWindowTitle("ETL Pipeline Manager")
        self.setGeometry(100, 100, 1050, 1080)
        self.setMinimumHeight(1080)
        self.setAcceptDrops(True)

        self.tab_widget = DetachableTabWidget(self)
        self.setCentralWidget(self.tab_widget)

        self.tab_widget.tab_detached.connect(self._on_tab_detached)

        # Track detached windows
        self._detached_windows = []
        self._hovering_item = None

        logger.info("Initialized tabbed main window")

    # -----------------------------------------------------
    # Tab Management
    # -----------------------------------------------------
    def add_tab(self, widget: QWidget, title: str, closable: bool = True):
        index = self.tab_widget.addTab(widget, title)
        self.tab_widget.setCurrentIndex(index)

        if closable:
            close_btn = QPushButton("×")
            close_btn.setObjectName("tab_close_btn")
            close_btn.setFixedSize(16, 16)
            close_btn.clicked.connect(lambda _, w=widget: self._close_tab_by_widget(w))
            self.tab_widget.tabBar().setTabButton(index, QTabBar.RightSide, close_btn)
        else:
            self.tab_widget.tabBar().setTabButton(index, QTabBar.RightSide, None)

        logger.info(f"Added tab: {title}")
        return index

    def _close_tab_by_widget(self, widget: QWidget):
        index = self.tab_widget.indexOf(widget)
        if index >= 0:
            self.tab_widget.removeTab(index)
            if hasattr(widget, "close"):
                widget.close()

    # -----------------------------------------------------
    # Detach / Reattach
    # -----------------------------------------------------
    def _on_tab_detached(self, widget: QWidget, title: str, position: QPoint):
        if isinstance(widget, QMainWindow):
            window = widget
            window.setWindowTitle(title)
        else:
            window = QMainWindow()
            window.setWindowTitle(title)
            window.setCentralWidget(widget)

        window.setGeometry(position.x(), position.y(), 1050, 1080)
        window.setMinimumHeight(1080)

        # Close handling
        window.closeEvent = lambda event: self._handle_detached_close(window, event)

        window.installEventFilter(self)
        window.show()

        self._detached_windows.append({
            "window": window,
            "widget": widget,
            "title": title
        })

        logger.info(f"Detached tab '{title}'")

    def _handle_detached_close(self, window: QMainWindow, event):
        for item in self._detached_windows[:]:
            if item["window"] == window:
                self._detached_windows.remove(item)

                widget = item["widget"]
                if hasattr(widget, "close"):
                    widget.close()

                logger.info(f"Detached window '{item['title']}' closed")
                break

        event.accept()

    def reattach_window(self, widget: QWidget, title: str):
        detached_window = None

        for item in self._detached_windows[:]:
            if item["widget"] == widget:
                detached_window = item["window"]
                self._detached_windows.remove(item)
                break

        if isinstance(widget, QMainWindow):
            if detached_window:
                detached_window.hide()
        else:
            if detached_window:
                detached_window.takeCentralWidget()
                detached_window.close()

        self.add_tab(widget, title)
        logger.info(f"Reattached '{title}'")

    # -----------------------------------------------------
    # Drop / Drag Handling
    # -----------------------------------------------------
    def dragEnterEvent(self, event):
        event.accept()

    def dragMoveEvent(self, event):
        event.accept()

    def dropEvent(self, event):
        for item in self._detached_windows[:]:
            if item["window"].underMouse():
                self.reattach_window(item["widget"], item["title"])
                break
        event.accept()

    # -----------------------------------------------------
    # Event Filtering for Hover Detection
    # -----------------------------------------------------
    def eventFilter(self, obj, event):
        for item in self._detached_windows[:]:
            if obj != item["window"]:
                continue

            if event.type() == QEvent.Type.Move:
                if self.geometry().intersects(obj.geometry()):
                    self.tab_widget.setStyleSheet(
                        "QTabBar { border: 3px solid #0d6efd; "
                        "background-color: rgba(13, 110, 253, 0.1); }"
                    )
                    self._hovering_item = item
                else:
                    self.tab_widget.setStyleSheet("")
                    self._hovering_item = None

            elif event.type() in (
                QEvent.Type.MouseButtonRelease,
                QEvent.Type.NonClientAreaMouseButtonRelease,
            ):
                if self._hovering_item == item:
                    if self.geometry().intersects(obj.geometry()):
                        self.reattach_window(item["widget"], item["title"])
                        self.tab_widget.setStyleSheet("")
                        self._hovering_item = None
                        return True

            elif event.type() == QEvent.Type.Close:
                self._handle_detached_close(obj, event)
                return False

        return super().eventFilter(obj, event)

    # -----------------------------------------------------
    # Utility Methods
    # -----------------------------------------------------
    def get_tab_by_title(self, title: str):
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == title:
                return self.tab_widget.widget(i)
        return None

    def close_tab(self, title: str):
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == title:
                self.tab_widget.removeTab(i)
                logger.info(f"Closed tab: {title}")
                return True
        return False
