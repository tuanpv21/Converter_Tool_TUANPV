#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Desktop GUI Tool: Presto <-> Spark SQL Bi-directional Transpiler (Tkinter)
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
import sys

try:
    from convert_presto_to_spark import convert_sql
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from convert_presto_to_spark import convert_sql


SAMPLE_PRESTO_SQL = """-- Vi du truy van Presto tren S3
SELECT 
    cast(cust_id as varchar) as cust_id_str,
    json_extract_scalar(event_payload, '$.device_info.os') as os_type,
    date_diff('day', date_parse(sign_up_date, '%Y-%m-%d'), current_date) as active_days,
    cardinality(transaction_ids) as total_txns,
    contains(transaction_ids, 'TXN_VIP_999') as is_vip_txn,
    array_join(tag_names, ', ') as tags_str,
    approx_distinct(session_token) as approx_sessions
FROM customer_activity_logs
WHERE date_add('day', -30, current_date) <= date_parse(log_date, '%Y-%m-%d')
GROUP BY 1, 2, 3, 4, 5, 6, 7;
"""

SAMPLE_SPARK_SQL = """-- Vi du truy van Spark SQL tren S3
SELECT 
    cast(cust_id as string) as cust_id_str,
    get_json_object(event_payload, '$.device_info.os') as os_type,
    datediff(current_date(), to_date(sign_up_date, 'yyyy-MM-dd')) as active_days,
    size(transaction_ids) as total_txns,
    array_contains(transaction_ids, 'TXN_VIP_999') as is_vip_txn,
    concat_ws(', ', tag_names) as tags_str,
    approx_count_distinct(session_token) as approx_sessions
FROM customer_activity_logs
WHERE date_add(current_date(), -30) <= to_date(log_date, 'yyyy-MM-dd')
GROUP BY 1, 2, 3, 4, 5, 6, 7;
"""

class BiDirectionalSQLApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQL Transpiler Tool 2 Chiều: Presto <-> Spark SQL - Public Bank Vietnam")
        self.title("SQL Transpiler Tool 2 Chiều: Presto <-> Spark SQL - TUANPV")
        self.geometry("1140x700")
        self.minsize(850, 520)

        # Mode: "presto2spark" or "spark2presto"
        self.current_mode = "presto2spark"

        # Fonts
        self.font_code = ("Consolas", 11)
        self.font_btn = ("Segoe UI", 10, "bold")
        self.font_label = ("Segoe UI", 10, "bold")

        self.setup_ui()

    def setup_ui(self):
        # 1. Top Header Bar
        top_bar = tk.Frame(self, bg="#0f172a", height=50)
        top_bar.pack(fill=tk.X, side=tk.TOP)
        top_bar.pack_propagate(False)

        title_lbl = tk.Label(
            top_bar, 
            text="⚡ TOOL CHUYỂN ĐỔI CÚ PHÁP SQL 2 CHIỀU: PRESTO ⇄ SPARK SQL",
            bg="#0f172a", 
            fg="#38bdf8", 
            font=("Segoe UI", 12, "bold")
        )
        title_lbl.pack(side=tk.LEFT, padx=15)

        engine_lbl = tk.Label(
            top_bar,
            text="Engine: sqlglot (AST Full Parser)",
            bg="#0f172a",
            fg="#94a3b8",
            font=("Segoe UI", 9, "italic")
        )
        engine_lbl.pack(side=tk.RIGHT, padx=15)

        # 2. Toolbar (Switch Direction & Quick Actions)
        toolbar = tk.Frame(self, bg="#f8fafc", pady=8, padx=15)
        toolbar.pack(fill=tk.X)

        self.btn_switch_mode = tk.Button(
            toolbar,
            text="⇄ ĐỔI CHIỀU: Presto ➔ Spark SQL",
            command=self.toggle_mode,
            font=("Segoe UI", 10, "bold"),
            bg="#0284c7",
            fg="white",
            padx=14,
            pady=4,
            relief=tk.FLAT,
            cursor="hand2"
        )
        self.btn_switch_mode.pack(side=tk.LEFT, padx=(0, 10))

        btn_swap_content = tk.Button(
            toolbar,
            text="⇆ Đổi chỗ nội dung 2 bên",
            command=self.swap_content,
            font=("Segoe UI", 9),
            bg="#e2e8f0",
            relief=tk.GROOVE,
            cursor="hand2"
        )
        btn_swap_content.pack(side=tk.LEFT, padx=(0, 15))

        self.mode_desc_var = tk.StringVar(value="Chế độ hiện tại: Nhập Presto SQL ➔ Chuyển thành Spark SQL")
        lbl_mode_desc = tk.Label(toolbar, textvariable=self.mode_desc_var, font=("Segoe UI", 9, "italic"), fg="#475569", bg="#f8fafc")
        lbl_mode_desc.pack(side=tk.LEFT)

        # 3. Middle Panels (Left: Input, Right: Output)
        paned = tk.PanedWindow(self, orient=tk.HORIZONTAL, sashrelief=tk.RAISED, sashwidth=6)
        paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)

        # === LEFT PANEL ===
        left_frame = tk.Frame(paned)
        paned.add(left_frame, weight=1)

        left_header = tk.Frame(left_frame)
        left_header.pack(fill=tk.X, pady=(0, 4))
        
        self.lbl_left = tk.Label(left_header, text="📥 INPUT: Presto SQL", font=self.font_label, fg="#0369a1")
        self.lbl_left.pack(side=tk.LEFT)

        btn_sample = tk.Button(left_header, text="Mẫu thử", command=self.load_sample, font=("Segoe UI", 8), bg="#e2e8f0")
        btn_sample.pack(side=tk.RIGHT, padx=2)

        btn_open = tk.Button(left_header, text="Mở File...", command=self.open_file, font=("Segoe UI", 8), bg="#e2e8f0")
        btn_open.pack(side=tk.RIGHT, padx=2)

        btn_clear = tk.Button(left_header, text="Xóa", command=self.clear_input, font=("Segoe UI", 8), bg="#fee2e2", fg="#991b1b")
        btn_clear.pack(side=tk.RIGHT, padx=2)

        self.txt_in = tk.Text(left_frame, wrap=tk.NONE, font=self.font_code, bg="#f8fafc", fg="#0f172a", undo=True)
        scroll_y_left = tk.Scrollbar(left_frame, orient=tk.VERTICAL, command=self.txt_in.yview)
        scroll_x_left = tk.Scrollbar(left_frame, orient=tk.HORIZONTAL, command=self.txt_in.xview)
        self.txt_in.configure(yscrollcommand=scroll_y_left.set, xscrollcommand=scroll_x_left.set)

        scroll_y_left.pack(side=tk.RIGHT, fill=tk.Y)
        scroll_x_left.pack(side=tk.BOTTOM, fill=tk.X)
        self.txt_in.pack(fill=tk.BOTH, expand=True)

        # === RIGHT PANEL ===
        right_frame = tk.Frame(paned)
        paned.add(right_frame, weight=1)

        right_header = tk.Frame(right_frame)
        right_header.pack(fill=tk.X, pady=(0, 4))

        self.lbl_right = tk.Label(right_header, text="📤 OUTPUT: Spark SQL", font=self.font_label, fg="#047857")
        self.lbl_right.pack(side=tk.LEFT)

        btn_copy = tk.Button(right_header, text="📋 Sao Chép (Copy)", command=self.copy_output, font=("Segoe UI", 8, "bold"), bg="#d1fae5", fg="#065f46")
        btn_copy.pack(side=tk.RIGHT, padx=2)

        btn_save = tk.Button(right_header, text="Lưu File...", command=self.save_file, font=("Segoe UI", 8), bg="#e2e8f0")
        btn_save.pack(side=tk.RIGHT, padx=2)

        self.txt_out = tk.Text(right_frame, wrap=tk.NONE, font=self.font_code, bg="#f0fdf4", fg="#0f172a", undo=True)
        scroll_y_right = tk.Scrollbar(right_frame, orient=tk.VERTICAL, command=self.txt_out.yview)
        scroll_x_right = tk.Scrollbar(right_frame, orient=tk.HORIZONTAL, command=self.txt_out.xview)
        self.txt_out.configure(yscrollcommand=scroll_y_right.set, xscrollcommand=scroll_x_right.set)

        scroll_y_right.pack(side=tk.RIGHT, fill=tk.Y)
        scroll_x_right.pack(side=tk.BOTTOM, fill=tk.X)
        self.txt_out.pack(fill=tk.BOTH, expand=True)

        # 4. Bottom Action & Status Bar
        bottom_frame = tk.Frame(self, pady=8, bg="#f1f5f9")
        bottom_frame.pack(fill=tk.X, side=tk.BOTTOM)

        self.btn_convert = tk.Button(
            bottom_frame,
            text="🚀 CHUYỂN ĐỔI SANG SPARK SQL (Ctrl + Enter)",
            command=self.do_convert,
            font=self.font_btn,
            bg="#2563eb",
            fg="white",
            padx=20,
            pady=7,
            relief=tk.FLAT,
            cursor="hand2"
        )
        self.btn_convert.pack(side=tk.LEFT, padx=15)

        self.status_var = tk.StringVar(value="Sẵn sàng. Dán code SQL vào khung bên trái và bấm Chuyển đổi.")
        lbl_status = tk.Label(bottom_frame, textvariable=self.status_var, font=("Segoe UI", 9), fg="#475569", bg="#f1f5f9")
        lbl_status.pack(side=tk.LEFT, padx=10)

        # Shortcuts
        self.bind("<Control-Return>", lambda event: self.do_convert())
        self.bind("<F5>", lambda event: self.do_convert())

        self.load_sample()

    def toggle_mode(self):
        if self.current_mode == "presto2spark":
            self.current_mode = "spark2presto"
            self.btn_switch_mode.config(text="⇄ ĐỔI CHIỀU: Spark SQL ➔ Presto", bg="#059669")
            self.lbl_left.config(text="📥 INPUT: Spark SQL", fg="#047857")
            self.lbl_right.config(text="📤 OUTPUT: Presto SQL", fg="#0369a1")
            self.btn_convert.config(text="🚀 CHUYỂN ĐỔI SANG PRESTO SQL (Ctrl + Enter)", bg="#059669")
            self.txt_out.config(bg="#f0f9ff")
            self.mode_desc_var.set("Chế độ hiện tại: Nhập Spark SQL ➔ Chuyển thành Presto / Trino")
        else:
            self.current_mode = "presto2spark"
            self.btn_switch_mode.config(text="⇄ ĐỔI CHIỀU: Presto ➔ Spark SQL", bg="#0284c7")
            self.lbl_left.config(text="📥 INPUT: Presto SQL", fg="#0369a1")
            self.lbl_right.config(text="📤 OUTPUT: Spark SQL", fg="#047857")
            self.btn_convert.config(text="🚀 CHUYỂN ĐỔI SANG SPARK SQL (Ctrl + Enter)", bg="#2563eb")
            self.txt_out.config(bg="#f0fdf4")
            self.mode_desc_var.set("Chế độ hiện tại: Nhập Presto SQL ➔ Chuyển thành Spark SQL")
        
        self.status_var.set(f"Đã chuyển sang chế độ: {self.current_mode.upper()}")

    def swap_content(self):
        """Đổi chỗ nội dung giữa Input và Output, đồng thời tự động đảo chiều convert"""
        in_text = self.txt_in.get("1.0", tk.END).strip()
        out_text = self.txt_out.get("1.0", tk.END).strip()

        self.txt_in.delete("1.0", tk.END)
        self.txt_out.delete("1.0", tk.END)

        self.txt_in.insert(tk.END, out_text)
        self.txt_out.insert(tk.END, in_text)

        self.toggle_mode()
        self.status_var.set("Đã đảo vị trí nội dung và chuyển hướng chuyển đổi!")

    def load_sample(self):
        self.txt_in.delete("1.0", tk.END)
        self.txt_out.delete("1.0", tk.END)
        if self.current_mode == "presto2spark":
            self.txt_in.insert(tk.END, SAMPLE_PRESTO_SQL.strip())
            self.status_var.set("Đã nạp câu truy vấn Presto mẫu.")
        else:
            self.txt_in.insert(tk.END, SAMPLE_SPARK_SQL.strip())
            self.status_var.set("Đã nạp câu truy vấn Spark SQL mẫu.")

    def clear_input(self):
        self.txt_in.delete("1.0", tk.END)
        self.txt_out.delete("1.0", tk.END)
        self.status_var.set("Đã xóa trắng.")

    def open_file(self):
        filepath = filedialog.askopenfilename(
            title="Chọn file SQL",
            filetypes=[("SQL Files", "*.sql"), ("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if filepath:
            try:
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                self.txt_in.delete("1.0", tk.END)
                self.txt_in.insert(tk.END, content)
                self.status_var.set(f"Đã mở file: {os.path.basename(filepath)}")
            except Exception as e:
                messagebox.showerror("Lỗi mở file", str(e))

    def save_file(self):
        content = self.txt_out.get("1.0", tk.END).strip()
        if not content:
            messagebox.showwarning("Cảnh báo", "Chưa có nội dung kết quả để lưu!")
            return

        filepath = filedialog.asksaveasfilename(
            title="Lưu file SQL",
            defaultextension=".sql",
            filetypes=[("SQL Files", "*.sql"), ("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if filepath:
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)
                self.status_var.set(f"Đã lưu kết quả ra: {os.path.basename(filepath)}")
                messagebox.showinfo("Thành công", "Đã lưu file thành công!")
            except Exception as e:
                messagebox.showerror("Lỗi lưu file", str(e))

    def copy_output(self):
        content = self.txt_out.get("1.0", tk.END).strip()
        if not content:
            messagebox.showwarning("Cảnh báo", "Không có nội dung để sao chép!")
            return
        self.clipboard_clear()
        self.clipboard_append(content)
        self.status_var.set("Đã sao chép kết quả vào Clipboard!")
        messagebox.showinfo("Thông báo", "Đã sao chép vào bộ nhớ đệm (Clipboard)!")

    def do_convert(self):
        query = self.txt_in.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Cảnh báo", "Vui lòng nhập câu lệnh SQL cần chuyển đổi!")
            return

        try:
            res = convert_sql(query, mode=self.current_mode)
            self.txt_out.delete("1.0", tk.END)
            self.txt_out.insert(tk.END, res)
            lines_in = len(query.splitlines())
            lines_out = len(res.splitlines())
            self.status_var.set(f"✅ Chuyển đổi thành công [{self.current_mode.upper()}] ({lines_in} dòng -> {lines_out} dòng)")
        except Exception as e:
            self.status_var.set(f"❌ Có lỗi xảy ra: {str(e)}")
            messagebox.showerror("Lỗi chuyển đổi", f"Chi tiết lỗi:\n{str(e)}")


def main():
    app = BiDirectionalSQLApp()
    app.mainloop()

if __name__ == "__main__":
    main()
