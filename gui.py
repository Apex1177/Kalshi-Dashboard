import threading
import queue
import asyncio
from datetime import datetime
import customtkinter as ctk

# Import from main module
from dashboard import (
    client,
    fetch_dashboard_data,
    update_ws_subscriptions,
    volume_recorder,
    save_volume_display_cache,
    get_reads_per_second,
    set_reads_per_second,
    clear_request_timestamps,
    REFRESH_INTERVAL,
    MAX_RETRIES,
    BASE_RETRY_DELAY,
    logger,
)
import aiohttp


class DynamicTable(ctk.CTkFrame):
    """Dynamic table that creates rows based on actual data."""
    
    def __init__(self, parent, title: str, columns: list, **kwargs):
        super().__init__(parent, **kwargs)
        self.columns = columns
        self.row_widgets = []  # List of row label lists
        
        # Title
        if title:
            self.title_label = ctk.CTkLabel(
                self, 
                text=title, 
                font=ctk.CTkFont(size=13, weight="bold")
            )
            self.title_label.grid(row=0, column=0, columnspan=len(columns), sticky="w", padx=8, pady=(6, 4))
        
        # Header row
        for col_idx, col in enumerate(columns):
            label = ctk.CTkLabel(
                self, 
                text=col['name'],
                font=ctk.CTkFont(size=11, weight="bold"),
                fg_color="gray25",
                corner_radius=0,
                width=col.get('width', 80),
                height=24,
                anchor="center"
            )
            label.grid(row=1, column=col_idx, padx=1, pady=(0, 1), sticky="ew")
        
        # Configure column weights
        for col_idx, col in enumerate(columns):
            self.grid_columnconfigure(col_idx, weight=col.get('weight', 1))
    
    def _format_value(self, value, col, row_data):
        """Format a cell value based on column settings."""
        fmt = col.get('format')
        if fmt == 'currency':
            if isinstance(value, (int, float)) and value != 0:
                return f"${value/100:.2f}"
            return "-"
        elif fmt == 'currency_signed':
            if isinstance(value, (int, float)):
                return f"${value/100:+.2f}" if value != 0 else "$0.00"
        elif fmt == 'number':
            if isinstance(value, (int, float)) and value > 0:
                return f"{value:,}"
            return "-"
        return str(value) if value else "-"
    
    def _get_text_color(self, col, row_data):
        """Get text color for a cell (e.g., P&L coloring)."""
        if col.get('key') == 'pnl' and isinstance(row_data.get('pnl'), (int, float)):
            pnl = row_data.get('pnl', 0)
            if pnl > 0:
                return "#4CAF50"
            elif pnl < 0:
                return "#F44336"
        return "white"
    
    def update_data(self, rows: list):
        """Update table with new data, creating/removing rows as needed."""
        # Remove excess rows
        while len(self.row_widgets) > len(rows):
            row_labels = self.row_widgets.pop()
            for label in row_labels:
                label.destroy()
        
        # Update existing rows and create new ones as needed
        for row_idx, row_data in enumerate(rows):
            if row_idx < len(self.row_widgets):
                # Update existing row
                for col_idx, col in enumerate(self.columns):
                    value = row_data.get(col['key'], '-')
                    formatted = self._format_value(value, col, row_data)
                    text_color = self._get_text_color(col, row_data)
                    self.row_widgets[row_idx][col_idx].configure(text=formatted, text_color=text_color)
            else:
                # Create new row
                row_labels = []
                bg_color = "gray20" if row_idx % 2 == 0 else "gray17"
                for col_idx, col in enumerate(self.columns):
                    value = row_data.get(col['key'], '-')
                    formatted = self._format_value(value, col, row_data)
                    text_color = self._get_text_color(col, row_data)
                    
                    label = ctk.CTkLabel(
                        self,
                        text=formatted,
                        font=ctk.CTkFont(size=11),
                        fg_color=bg_color,
                        corner_radius=0,
                        width=col.get('width', 80),
                        height=22,
                        anchor="center",
                        text_color=text_color
                    )
                    label.grid(row=row_idx + 2, column=col_idx, padx=1, pady=0, sticky="ew")
                    row_labels.append(label)
                self.row_widgets.append(row_labels)
        
        # Show placeholder if no data
        if not rows:
            if not self.row_widgets:
                row_labels = []
                for col_idx, col in enumerate(self.columns):
                    text = "No data" if col_idx == 0 else "-"
                    label = ctk.CTkLabel(
                        self,
                        text=text,
                        font=ctk.CTkFont(size=11),
                        fg_color="gray20",
                        corner_radius=0,
                        width=col.get('width', 80),
                        height=22,
                        anchor="center",
                        text_color="gray50"
                    )
                    label.grid(row=2, column=col_idx, padx=1, pady=0, sticky="ew")
                    row_labels.append(label)
                self.row_widgets.append(row_labels)


class DashboardWindow(ctk.CTk):
    """Main dashboard window - compact vertical layout."""
    
    def __init__(self, data_queue: queue.Queue):
        super().__init__()
        
        self.data_queue = data_queue
        
        # Configure window
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        self.title("Kalshi Portfolio Dashboard")
        self.geometry("950x900")
        self.minsize(850, 700)
        
        # Header (fixed at top)
        self.header_container = ctk.CTkFrame(self, fg_color="transparent")
        self.header_container.pack(fill="x", padx=10, pady=(10, 0))
        
        # Scrollable content area for tables
        self.main_frame = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Status bar (fixed at bottom)
        self.status_container = ctk.CTkFrame(self, fg_color="transparent")
        self.status_container.pack(fill="x", padx=10, pady=(0, 10))
        
        # Build UI sections
        self._build_header()
        self._build_totals_bar()
        self._build_exposure_section()
        self._build_queue_section()
        self._build_volume_section()
        self._build_status_bar()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._last_volume_rows = []
        # Start checking for data updates
        self.after(100, self._check_queue)

    def _on_close(self):
        """Flush volume data, save volume display cache, and quit on window close."""
        try:
            save_volume_display_cache(self._last_volume_rows)
        except Exception as e:
            logger.warning("Failed to save volume display cache on shutdown: %s", e)
        try:
            volume_recorder.flush()
        except Exception as e:
            logger.warning("Failed to flush volume recorder on shutdown: %s", e)
        self.quit()
        self.destroy()
    
    def _build_header(self):
        """Build compact header."""
        header = ctk.CTkFrame(self.header_container)
        header.pack(fill="x", pady=(0, 5))
        
        # Title and info on same row
        ctk.CTkLabel(
            header,
            text="KALSHI PORTFOLIO",
            font=ctk.CTkFont(size=16, weight="bold")
        ).pack(side="left", padx=15, pady=8)
        
        self.markets_label = ctk.CTkLabel(
            header,
            text="Markets: 0",
            font=ctk.CTkFont(size=12)
        )
        self.markets_label.pack(side="right", padx=15)
        
        self.balance_label = ctk.CTkLabel(
            header,
            text="Balance: $0.00",
            font=ctk.CTkFont(size=12)
        )
        self.balance_label.pack(side="right", padx=15)
    
    def _build_totals_bar(self):
        """Build totals bar at top showing Net Risk and P&L."""
        totals_frame = ctk.CTkFrame(self.header_container, fg_color="gray25", height=28)
        totals_frame.pack(fill="x", pady=(0, 5))
        totals_frame.pack_propagate(False)
        
        self.totals_label = ctk.CTkLabel(
            totals_frame,
            text="Net Risk: $0.00 | P&L: $0.00",
            font=ctk.CTkFont(size=11)
        )
        self.totals_label.pack(pady=4)
    
    def _build_exposure_section(self):
        """Build market exposure table."""
        columns = [
            {'name': 'Market', 'key': 'ticker', 'width': 220, 'weight': 3},
            {'name': 'YES', 'key': 'yes_exp', 'width': 85, 'weight': 1, 'format': 'currency'},
            {'name': 'NO', 'key': 'no_exp', 'width': 85, 'weight': 1, 'format': 'currency'},
            {'name': 'Net Risk', 'key': 'max_risk', 'width': 85, 'weight': 1, 'format': 'currency'},
            {'name': 'P&L', 'key': 'pnl', 'width': 85, 'weight': 1, 'format': 'currency_signed'},
        ]
        
        self.exposure_table = DynamicTable(self.main_frame, "EXPOSURE", columns)
        self.exposure_table.pack(fill="x", pady=(0, 8))
    
    def _build_queue_section(self):
        """Build order queue table."""
        columns = [
            {'name': 'Market', 'key': 'ticker', 'width': 220, 'weight': 3},
            {'name': 'YES Queue', 'key': 'yes_q', 'width': 110, 'weight': 1},
            {'name': 'NO Queue', 'key': 'no_q', 'width': 110, 'weight': 1},
            {'name': 'Bid/Ask', 'key': 'bbo', 'width': 120, 'weight': 1},
        ]
        
        self.queue_table = DynamicTable(self.main_frame, "ORDER QUEUE", columns)
        self.queue_table.pack(fill="x", pady=(0, 8))
    
    def _build_volume_section(self):
        """Build trading volume table."""
        columns = [
            {'name': 'Market', 'key': 'ticker', 'width': 180, 'weight': 3},
            {'name': '1m', 'key': 'vol_1m', 'width': 60, 'weight': 1, 'format': 'number'},
            {'name': '10m', 'key': 'vol_10m', 'width': 60, 'weight': 1, 'format': 'number'},
            {'name': '1h', 'key': 'vol_1h', 'width': 60, 'weight': 1, 'format': 'number'},
            {'name': '6h', 'key': 'vol_6h', 'width': 60, 'weight': 1, 'format': 'number'},
            {'name': '12h', 'key': 'vol_12h', 'width': 60, 'weight': 1, 'format': 'number'},
            {'name': '24h', 'key': 'vol_24h', 'width': 70, 'weight': 1, 'format': 'number'},
            {'name': '24h $', 'key': 'vol_24h_dollars', 'width': 90, 'weight': 1, 'format': 'currency'},
        ]
        
        # Volume section with WebSocket status in title
        vol_header = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        vol_header.pack(fill="x")
        
        ctk.CTkLabel(
            vol_header,
            text="VOLUME",
            font=ctk.CTkFont(size=13, weight="bold")
        ).pack(side="left", padx=8)
        
        self.ws_status_label = ctk.CTkLabel(
            vol_header,
            text="○ WS",
            font=ctk.CTkFont(size=11),
            text_color="gray50"
        )
        self.ws_status_label.pack(side="right", padx=8)
        
        self.volume_table = DynamicTable(self.main_frame, "", columns)
        self.volume_table.pack(fill="x", pady=(0, 8))
    
    def _build_status_bar(self):
        """Build status bar."""
        status_frame = ctk.CTkFrame(self.status_container, fg_color="gray20", height=36)
        status_frame.pack(fill="x")
        status_frame.pack_propagate(False)

        left = ctk.CTkFrame(status_frame, fg_color="transparent")
        left.pack(side="left", fill="x", expand=True, padx=8, pady=6)
        self.status_label = ctk.CTkLabel(
            left,
            text="Starting...",
            font=ctk.CTkFont(size=11)
        )
        self.status_label.pack(anchor="w")

        right = ctk.CTkFrame(status_frame, fg_color="transparent")
        right.pack(side="right", padx=8, pady=6)
        ctk.CTkLabel(right, text="Read/s:", font=ctk.CTkFont(size=11)).pack(side="left", padx=(0, 4))
        self.reads_per_sec_entry = ctk.CTkEntry(
            right, width=50, height=24, font=ctk.CTkFont(size=11),
            placeholder_text="2.5"
        )
        self.reads_per_sec_entry.pack(side="left", padx=(0, 4))
        self.reads_per_sec_entry.insert(0, str(get_reads_per_second()))
        self.apply_reads_btn = ctk.CTkButton(
            right, text="Apply", width=56, height=24, font=ctk.CTkFont(size=11),
            command=self._on_apply_reads_per_sec
        )
        self.apply_reads_btn.pack(side="left", padx=(0, 6))
        self.rate_feedback_label = ctk.CTkLabel(
            right, text="", font=ctk.CTkFont(size=10), width=48, anchor="w"
        )
        self.rate_feedback_label.pack(side="left")

    def _on_apply_reads_per_sec(self):
        """Apply Read/s value from entry."""
        try:
            val = self.reads_per_sec_entry.get().strip()
            rps = float(val) if val else 2.5
        except ValueError:
            self._show_rate_feedback("Invalid", is_ok=False)
            return
        if set_reads_per_second(rps):
            clear_request_timestamps()
            self._show_rate_feedback("Applied")
        else:
            self._show_rate_feedback("Invalid", is_ok=False)

    def _show_rate_feedback(self, msg: str, is_ok: bool = True):
        """Show brief feedback next to Apply; clear after 2s."""
        self.rate_feedback_label.configure(
            text=msg,
            text_color="#4CAF50" if is_ok else "#F44336"
        )
        self.after(2000, lambda: self.rate_feedback_label.configure(text=""))
    
    def _check_queue(self):
        """Check for data updates from async thread."""
        try:
            data = self.data_queue.get_nowait()
            self._update_display(data)
        except queue.Empty:
            pass
        self.after(100, self._check_queue)
    
    def _update_display(self, data: dict):
        """Update all UI elements with new data."""
        if data.get('error'):
            self.status_label.configure(
                text=f"Error: {data['error'][:50]}... Retrying",
                text_color="#F44336"
            )
            return
        
        # Header
        balance = data.get('balance', 0)
        self.balance_label.configure(text=f"Balance: ${balance/100:.2f}")
        self.markets_label.configure(text=f"Markets: {data.get('active_markets', 0)}")
        
        # Exposure table
        exposure = data.get('exposure', {})
        exposure_rows = exposure.get('rows', [])
        self.exposure_table.update_data(exposure_rows)
        
        # Totals (Net Risk and P&L only)
        t = exposure.get('totals', {})
        self.totals_label.configure(
            text=f"Net Risk: ${t.get('max_risk', 0)/100:.2f} | "
                 f"P&L: ${t.get('pnl', 0)/100:+.2f}"
        )
        
        # Queue table
        queue_rows = []
        for row in exposure_rows:
            bbo = f"{row['bid']}/{row['ask']} ({row['spread']}¢)" if row.get('bid', 0) > 0 else "-"
            queue_rows.append({
                'ticker': row['ticker'],
                'yes_q': row.get('yes_q', '-'),
                'no_q': row.get('no_q', '-'),
                'bbo': bbo
            })
        self.queue_table.update_data(queue_rows)
        
        # Volume table - filter to rows with any volume data
        volume = data.get('volume', {})
        all_volume_rows = volume.get('rows', [])
        self._last_volume_rows = all_volume_rows
        volume_rows = [r for r in all_volume_rows
                       if any(r.get(k, 0) > 0 for k in ['vol_1m', 'vol_10m', 'vol_1h', 'vol_6h', 'vol_12h', 'vol_24h'])]
        self.volume_table.update_data(volume_rows or [{'ticker': 'Collecting...', 'vol_1m': 0, 'vol_10m': 0, 'vol_1h': 0, 'vol_6h': 0, 'vol_12h': 0, 'vol_24h': 0, 'vol_24h_dollars': 0}])
        
        # WebSocket status
        if volume.get('ws_connected', False):
            self.ws_status_label.configure(text="● WS", text_color="#4CAF50")
        else:
            self.ws_status_label.configure(text="○ WS", text_color="gray50")
        
        # Status bar
        ts = data.get('timestamp', datetime.now())
        time_str = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else str(ts)
        self.status_label.configure(
            text=f"Updated: {time_str} | Cached",
            text_color="white"
        )


async def fetch_loop(data_queue: queue.Queue):
    """Async loop that fetches data and puts it in the queue."""
    consecutive_failures = 0
    ws_task = None
    
    while True:
        try:
            async with client:
                async with aiohttp.ClientSession() as http_session:
                    while True:
                        try:
                            data = await fetch_dashboard_data(http_session)
                            data_queue.put(data)
                            
                            tickers = data.get('tickers', [])
                            ws_task = await update_ws_subscriptions(ws_task, tickers)
                            
                            consecutive_failures = 0
                            await asyncio.sleep(REFRESH_INTERVAL)
                            
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            consecutive_failures += 1
                            logger.warning(f"Dashboard fetch error (attempt {consecutive_failures}): {e}")
                            data_queue.put({'error': str(e)})
                            
                            if consecutive_failures >= MAX_RETRIES:
                                logger.error(f"Max retries reached, reconnecting...")
                                break
                            
                            delay = min(BASE_RETRY_DELAY * (2 ** (consecutive_failures - 1)), 60)
                            await asyncio.sleep(delay)
                            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Unexpected error in fetch loop: {e}")
            await asyncio.sleep(2)


def run_async_in_thread(data_queue: queue.Queue):
    """Run the async event loop in a background thread."""
    asyncio.run(fetch_loop(data_queue))


def run_gui():
    """Main entry point for GUI mode."""
    data_queue = queue.Queue()
    
    async_thread = threading.Thread(target=run_async_in_thread, args=(data_queue,), daemon=True)
    async_thread.start()
    
    app = DashboardWindow(data_queue)
    app.mainloop()


if __name__ == "__main__":
    run_gui()
