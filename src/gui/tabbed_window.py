"""
Tabbed main window with detachable tabs.
Allows windows to be used as tabs or dragged out as standalone windows.
"""

from PySide6.QtWidgets import QMainWindow, QTabWidget, QTabBar, QWidget, QPushButton
from PySide6.QtCore import Qt, Signal, QPoint
import logging

logger = logging.getLogger(__name__)


class DetachableTabBar(QTabBar):
    """Tab bar that allows tabs to be detached into separate windows."""

    tab_detached = Signal(int, QPoint)  # tab_index, global_position

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setElideMode(Qt.ElideRight)
        self.setSelectionBehaviorOnRemove(QTabBar.SelectLeftTab)
        self.setMovable(True)

        self.drag_start_pos = QPoint()
        self.drag_initiated = False

    def mousePressEvent(self, event):
        """Handle mouse press to start potential drag."""
        if event.button() == Qt.LeftButton:
            self.drag_start_pos = event.pos()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        """Handle mouse move to detect drag."""
        if not (event.buttons() & Qt.LeftButton):
            super().mouseMoveEvent(event)
            return

        if (event.pos() - self.drag_start_pos).manhattanLength() < 30:
            super().mouseMoveEvent(event)
            return

        # Start drag operation
        tab_index = self.tabAt(self.drag_start_pos)
        if tab_index < 0:
            super().mouseMoveEvent(event)
            return

        # Emit signal to detach tab
        global_pos = self.mapToGlobal(event.pos())
        self.tab_detached.emit(tab_index, global_pos)

        event.accept()


class DetachableTabWidget(QTabWidget):
    """Tab widget with detachable tabs that become standalone windows."""

    tab_detached = Signal(QWidget, str, QPoint)  # widget, title, position

    def __init__(self, parent=None):
        super().__init__(parent)

        # Use custom tab bar
        self.tab_bar = DetachableTabBar(self)
        self.setTabBar(self.tab_bar)

        # Connect tab detach signal
        self.tab_bar.tab_detached.connect(self._on_tab_detach_requested)

        # Enable close buttons on tabs
        self.setTabsClosable(True)
        self.tabCloseRequested.connect(self._on_tab_close_requested)

    def _on_tab_detach_requested(self, tab_index, position):
        """Handle tab detachment request."""
        if tab_index < 0 or tab_index >= self.count():
            return

        # Get widget and title before removing
        widget = self.widget(tab_index)
        title = self.tabText(tab_index)

        # Important: Set parent to None before removing to prevent deletion
        widget.setParent(None)

        # Remove from tab widget
        self.removeTab(tab_index)

        # Emit signal for parent to handle
        self.tab_detached.emit(widget, title, position)

    def _on_tab_close_requested(self, tab_index):
        """Handle tab close request."""
        widget = self.widget(tab_index)
        self.removeTab(tab_index)

        # Close the widget if it's not the main dashboard
        if hasattr(widget, 'close'):
            widget.close()


class TabbedMainWindow(QMainWindow):
    """
    Main window with tabbed interface and detachable tabs.
    Windows can be used as tabs or dragged out as standalone windows.
    """

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Pipeline Manager")
        self.setGeometry(100, 100, 1050, 1080)
        self.setMinimumHeight(1080)  # Keep minimum height, but allow width to be adjustable

        # Enable drop events for reattaching tabs
        self.setAcceptDrops(True)

        # Create tab widget
        self.tab_widget = DetachableTabWidget(self)
        self.setCentralWidget(self.tab_widget)

        # Connect detach signal
        self.tab_widget.tab_detached.connect(self._on_tab_detached)

        # Track detached windows
        self.detached_windows = []
        self._hovering_window = None

        logger.info("Initialized tabbed main window")

    def add_tab(self, widget, title, closable=True):
        """
        Add a new tab.
        
        Args:
            widget: Widget to add as tab
            title: Tab title
            closable: Whether the tab can be closed
        """
        index = self.tab_widget.addTab(widget, title)
        self.tab_widget.setCurrentIndex(index)

        # Add custom close button with × icon
        if closable:
            close_btn = QPushButton("×")
            close_btn.setObjectName("tab_close_btn")
            close_btn.setFixedSize(16, 16)
            close_btn.clicked.connect(lambda checked=False, w=widget: self._close_tab_by_widget(w))
            self.tab_widget.tabBar().setTabButton(index, QTabBar.RightSide, close_btn)
        else:
            # Disable close button for this specific tab
            self.tab_widget.tabBar().setTabButton(index, QTabBar.RightSide, None)

        logger.info(f"Added tab: {title}")
        return index

    def _close_tab_by_widget(self, widget):
        """Close a tab by its widget."""
        index = self.tab_widget.indexOf(widget)
        if index >= 0:
            self.tab_widget.removeTab(index)
            # Close the widget if it has a close method
            if hasattr(widget, 'close'):
                widget.close()

    def _on_tab_detached(self, widget, title, position):
        """Handle tab being detached into standalone window."""
        # Check if widget is already a QMainWindow (like ETLMainWindow)
        if isinstance(widget, QMainWindow):
            # Just show the existing window at the position
            detached_window = widget
            detached_window.setWindowTitle(title)
            detached_window.setGeometry(position.x(), position.y(), 1050, 1080)
            detached_window.setMinimumHeight(1080)  # Allow width to be adjustable
        else:
            # Create standalone window for widget-based content
            detached_window = QMainWindow()
            detached_window.setWindowTitle(title)
            detached_window.setCentralWidget(widget)
            detached_window.setGeometry(position.x(), position.y(), 1050, 1080)
            detached_window.setMinimumHeight(1080)  # Allow width to be adjustable
            widget.show()  # Ensure widget is visible

        # Connect close event to cleanup
        detached_window.closeEvent = lambda event: self._handle_detached_window_close(detached_window, event)

        # Make window draggable for reattachment
        detached_window.installEventFilter(self)

        # Show the detached window
        detached_window.show()

        # Track it so we can reattach if needed
        self.detached_windows.append({
            'window': detached_window,
            'widget': widget,
            'title': title
        })

        logger.info(f"Detached tab '{title}' to standalone window")

    def _on_detached_window_closed(self, window):
        """Handle detached window being closed."""
        # Find the window in our tracking list
        for item in self.detached_windows[:]:
            if item['window'] == window:
                self.detached_windows.remove(item)
                logger.info(f"Detached window '{item['title']}' closed")
                break

    def _handle_detached_window_close(self, window, event):
        """Handle close event for detached window."""
        # Remove from tracking
        for item in self.detached_windows[:]:
            if item['window'] == window:
                self.detached_windows.remove(item)
                widget = item['widget']

                # Clean up the widget
                if hasattr(widget, 'close'):
                    widget.close()

                logger.info(f"Detached window '{item['title']}' closed by user")
                break

        # Accept the close event
        event.accept()

    def reattach_window(self, widget, title):
        """
        Reattach a detached window as a tab.
        
        Args:
            widget: Widget to reattach
            title: Tab title
        """
        # Find and remove from detached windows list
        detached_window = None
        for item in self.detached_windows[:]:
            if item['widget'] == widget:
                detached_window = item['window']
                self.detached_windows.remove(item)
                break

        # If widget is a QMainWindow, we need to handle it differently
        if isinstance(widget, QMainWindow):
            # For QMainWindow (like ETLMainWindow), just hide the detached window
            if detached_window:
                detached_window.hide()
        else:
            # For QWidget (like UserManagementDialog), we need to:
            # 1. Remove it from the wrapper window's central widget
            # 2. Close the wrapper window
            if detached_window and detached_window != widget:
                detached_window.takeCentralWidget()  # Remove widget from wrapper
                detached_window.close()  # Close the wrapper window

        # Add back as tab
        self.add_tab(widget, title)
        logger.info(f"Reattached window '{title}' as tab")

    def get_tab_by_title(self, title):
        """Get tab widget by title."""
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == title:
                return self.tab_widget.widget(i)
        return None

    def close_tab(self, title):
        """Close a tab by title."""
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == title:
                self.tab_widget.removeTab(i)
                logger.info(f"Closed tab: {title}")
                return True
        return False

    def dragEnterEvent(self, event):
        """Accept drag events when detached window hovers over main window"""
        event.accept()

    def dragMoveEvent(self, event):
        """Handle drag move to show visual feedback"""
        event.accept()

    def dropEvent(self, event):
        """Handle drop event to reattach window"""
        # Check if we have a detached window being dragged
        for item in self.detached_windows[:]:
            window = item['window']
            # Check if the window is under the cursor
            if window.underMouse() or self.geometry().contains(event.position().toPoint()):
                self.reattach_window(item['widget'], item['title'])
                break
        event.accept()

    def eventFilter(self, obj, event):
        """Filter events from detached windows to detect hover over main window"""
        from PySide6.QtCore import QEvent

        # Check if it's a detached window
        for item in self.detached_windows[:]:
            if obj == item['window']:
                # If window is being moved and hovers over main window
                if event.type() == QEvent.Type.Move:
                    # Check if detached window overlaps with main window
                    if self.geometry().intersects(obj.geometry()):
                        # Visual feedback: highlight tab bar
                        self.tab_widget.setStyleSheet("QTabBar { border: 3px solid #0d6efd; background-color: rgba(13, 110, 253, 0.1); }")
                        # Store which window is hovering
                        self._hovering_window = item
                    else:
                        self.tab_widget.setStyleSheet("")
                        self._hovering_window = None

                # Detect when window stops being dragged (mouse release)
                elif event.type() == QEvent.Type.NonClientAreaMouseButtonRelease or event.type() == QEvent.Type.MouseButtonRelease:
                    # If window was hovering over main window, reattach it
                    if hasattr(self, '_hovering_window') and self._hovering_window == item:
                        if self.geometry().intersects(obj.geometry()):
                            self.reattach_window(item['widget'], item['title'])
                            self.tab_widget.setStyleSheet("")
                            self._hovering_window = None
                            return True

                # Handle window close button
                elif event.type() == QEvent.Type.Close:
                    self._on_detached_window_closed(obj)
                    return False  # Allow the window to close

        return super().eventFilter(obj, event)

    def mouseReleaseEvent(self, event):
        """Handle mouse release - not used since we track in eventFilter"""
        super().mouseReleaseEvent(event)
