"""Sales gauge chart widget"""

from PySide6.QtWidgets import QWidget
from PySide6.QtCore import Qt
from PySide6.QtGui import QPainter, QColor, QFont, QPen, QBrush
import math


class SalesGaugeWidget(QWidget):
    """Circular gauge chart showing sales progress towards goal"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_sales = 0.0
        self.sales_goal = 500000.0  # Default goal: 500,000 kr
        self.setMinimumSize(300, 300)
    
    def set_sales_data(self, current: float, goal: float = None):
        """Update sales data and repaint"""
        self.current_sales = current
        if goal is not None:
            self.sales_goal = goal
        self.update()
    
    def format_dkk(self, amount: float) -> str:
        """Format number as DKK with European formatting"""
        # Round to nearest whole number
        rounded = int(round(amount))
        # Format with period as thousands separator
        formatted = f"{rounded:,}".replace(",", ".")
        return f"{formatted} kr"
    
    def paintEvent(self, event):
        """Draw the gauge chart"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Get widget dimensions
        width = self.width()
        height = self.height()
        size = min(width, height)
        
        # Center point
        cx = width / 2
        cy = height / 2
        
        # Gauge parameters
        radius = size * 0.35
        thickness = size * 0.08
        
        # Calculate progress (0.0 to 1.0)
        progress = min(self.current_sales / self.sales_goal, 1.0) if self.sales_goal > 0 else 0
        
        # Draw background arc (gray)
        painter.setPen(QPen(QColor(200, 200, 200), thickness, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
        painter.drawArc(
            int(cx - radius), int(cy - radius),
            int(radius * 2), int(radius * 2),
            45 * 16, 270 * 16  # Start at 45°, span 270°
        )
        
        # Draw progress arc (blue/green gradient based on progress)
        if progress > 0:
            # Color changes from blue to green as progress increases
            if progress < 0.5:
                color = QColor(66, 165, 245)  # Blue
            elif progress < 0.8:
                color = QColor(102, 187, 106)  # Light green
            else:
                color = QColor(76, 175, 80)  # Green
            
            painter.setPen(QPen(color, thickness, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            span_angle = int(270 * progress * 16)
            painter.drawArc(
                int(cx - radius), int(cy - radius),
                int(radius * 2), int(radius * 2),
                45 * 16, span_angle
            )
        
        # Draw center text - Current sales (GREEN)
        painter.setPen(QColor(76, 175, 80))  # Green color
        font = QFont("Arial", int(size * 0.06), QFont.Weight.Bold)
        painter.setFont(font)
        current_text = self.format_dkk(self.current_sales)
        text_rect = painter.boundingRect(0, 0, width, height, Qt.AlignmentFlag.AlignCenter, current_text)
        painter.drawText(int(cx - text_rect.width() / 2), int(cy), current_text)
        
        # Draw goal text below
        font_small = QFont("Arial", int(size * 0.04))
        painter.setFont(font_small)
        painter.setPen(QColor(120, 120, 120))
        goal_text = f"Goal: {self.format_dkk(self.sales_goal)}"
        goal_rect = painter.boundingRect(0, 0, width, height, Qt.AlignmentFlag.AlignCenter, goal_text)
        painter.drawText(int(cx - goal_rect.width() / 2), int(cy + 30), goal_text)
        
        painter.end()
