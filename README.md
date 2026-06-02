# Distributed Snapshot Isolation (Retail Inventory Sync)

Mô phỏng hệ thống giao dịch phân tán hỗ trợ cơ chế cô lập **Snapshot Isolation (SI)** trên cơ sở dữ liệu phân mảnh ngang (Horizontal Fragmentation). Dự án tích hợp giao thức **Cam kết Hai pha (2-Phase Commit - 2PC)** và quy tắc **First-Committer-Wins (FCW)** nhằm phát hiện xung đột đồng thời, giải quyết lỗi lệch ghi (**Write Skew Anomaly**) bằng cơ chế *Materializing Conflicts* và tự động khôi phục dữ liệu khi nút mạng gặp sự cố (**Crash Recovery**).

---

## 📁 Cấu trúc thư mục dự án

```
DoAnCuoiKi/
├── config/
│   └── config.json             # Cấu hình mạng, phân mảnh chẵn/lẻ và latency giả lập
├── src/
│   ├── coordinator/
│   │   ├── coordinator.py      # Global Transaction Coordinator & Centralized TSG
│   │   └── state.py            # Quản lý trạng thái giao dịch tập trung (persistence JSON)
│   ├── nodes/
│   │   ├── node_server.py      # API Server đại diện cho các site lưu trữ phân mảnh
│   │   └── storage.py          # Quản lý SQLite cục bộ với MVCC (PREPARED / COMMITTED)
│   ├── client/
│   │   ├── client.py           # Client SDK hỗ trợ begin(), read(), write(), commit()
│   │   ├── test_write_skew.py  # Kịch bản kiểm thử chặn lỗi Write Skew chéo site
│   │   └── test_concurrency.py # Kịch bản benchmark đo lường hiệu năng đồng thời
│   └── utils/
│       ├── logger.py           # Ghi logs trace hệ thống có màu sắc trực quan
│       └── seed_db.py          # Script seed dữ liệu kho hàng ban đầu
├── requirements.txt            # Danh sách thư viện Python cần cài đặt
└── README.md                   # Hướng dẫn này
```

---

## 🛠️ Cài đặt & Vận hành

### Yêu cầu hệ thống
* Python 3.10+

### Bước 1: Cài đặt thư viện
Mở terminal tại thư mục gốc của dự án và chạy:
```bash
pip install -r requirements.txt
```

### Bước 2: Khởi động các tiến trình hệ thống
Bạn cần mở **3 cửa sổ Terminal độc lập** để chạy 3 tiến trình mạng chạy nền:

1. **Khởi động Site 1 - Store Front (Cổng 8001):**
   ```bash
   python src/nodes/node_server.py --node store_front
   ```
2. **Khởi động Site 2 - Back Office (Cổng 8002):**
   ```bash
   python src/nodes/node_server.py --node back_office
   ```
3. **Khởi động Global Coordinator (Cổng 8000):**
   ```bash
   python src/coordinator/coordinator.py
   ```

---

## 📊 Chạy các kịch bản kiểm thử (Demo Scenarios)

Sau khi 3 tiến trình trên đã hoạt động ổn định, bạn mở **Terminal thứ 4** để thực hiện kiểm thử:

### 0. Seed dữ liệu ban đầu
Khởi tạo dữ liệu kho hàng chẵn/lẻ chéo site:
```bash
python src/utils/seed_db.py
```

### 1. Minh chứng và Ngăn chặn lỗi Write Skew Anomaly
Mô phỏng lỗi Write Skew chéo site khi T1 trừ 1 tồn kho tại Store Front, T2 trừ 1 tồn kho tại Back Office dưới ràng buộc tổng tồn kho ≥ 10:
```bash
python src/client/test_write_skew.py
```
* **Chế độ SI thuần túy (Không bảo vệ):** Cả 2 giao dịch commit thành công $\to$ tổng tồn kho thực tế bị tụt xuống còn 9 (Vi phạm ràng buộc).
* **Chế độ bật bảo vệ (Materializing Conflicts):** Cả 2 giao dịch buộc phải Dummy Update lên khóa ràng buộc dùng chung. T1 commit trước thành công. T2 commit sau bị **Abort** do xung đột First-Committer-Wins cục bộ. Tổng tồn kho được bảo toàn ở mức 10.

### 2. Đo lường Hiệu năng & Vẽ biểu đồ Đồng thời (Benchmark)
Giả lập áp lực tăng dần từ N = 2, 5, 10, 20, 50 clients đồng thời ghi đè lên cùng một sản phẩm:
```bash
python src/client/test_concurrency.py
```
* Kết quả chi tiết dạng số liệu được lưu trữ tự động tại `output/benchmark_results.csv`.
* Ba biểu đồ trực quan hóa hiệu năng tự động kết xuất dưới dạng hình ảnh tại:
  * `output/abort_rate_chart.png` (Tỉ lệ Abort tăng dần theo N do quy tắc FCW).
  * `output/tps_chart.png` (Thông lượng giao dịch thành công).
  * `output/latency_chart.png` (Độ trễ trung bình khi xung đột tăng cao).

### 3. Phục hồi giao dịch sau sự cố (Crash Recovery)
Dự án tích hợp cơ chế tự phục hồi tự động khi nút lưu trữ bị sập nguồn khi đang ở trạng thái chuẩn bị cam kết dữ liệu (`PREPARED`):
1. Đảm bảo cả 3 server đang chạy và đã được seed dữ liệu.
2. Chạy tập lệnh nạp giao dịch treo vào nút Back Office:
   ```bash
   python src/client/test_crash_recovery.py
   ```
3. Nhấn `Ctrl + C` tại Terminal 2 để ngắt kết nối (kill) Back Office Node.
4. Chờ 3 giây, sau đó khởi động lại Back Office:
   ```bash
   python src/nodes/node_server.py --node back_office
   ```
5. **Quan sát Logs tại Terminal 2:** Back Office Node khi startup sẽ tự quét SQLite cục bộ, phát hiện giao dịch treo, chủ động kết nối hỏi Coordinator trạng thái giao dịch, nhận quyết định `ABORTED` và tự động thực thi **UNDO** làm sạch dữ liệu tạm để khôi phục tính nhất quán hoàn hảo.
