import sqlite3
import os
import json
from typing import Dict, List, Optional, Tuple

class LocalStorage:
    """Quản lý CSDL SQLite cục bộ cho mỗi Site phân mảnh, hỗ trợ MVCC và 2-Phase Commit."""

    def __init__(self, db_path: str, node_id: str):
        self.db_path = db_path
        self.node_id = node_id
        # Đảm bảo thư mục lưu dữ liệu tồn tại
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Khởi tạo cấu trúc các bảng dữ liệu SQLite phục vụ MVCC và 2PC."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Bảng lưu trữ sản phẩm MVCC
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Product_Inventory (
                    product_id INTEGER,
                    product_name TEXT,
                    stock_level INTEGER,
                    xmin INTEGER,              -- Nhãn thời gian commit hoặc Tx_ID tạm thời
                    xmax INTEGER,              -- Nhãn thời gian hết hạn phiên bản
                    state TEXT,                -- 'PREPARED' hoặc 'COMMITTED'
                    PRIMARY KEY (product_id, xmin)
                )
            """)
            
            # 2. Bảng ràng buộc dùng chung để cụ thể hóa xung đột (Materialize Conflicts) nhằm chặn Write Skew
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Inventory_Constraint (
                    constraint_id TEXT,
                    last_tx_id TEXT,
                    xmin INTEGER,
                    xmax INTEGER,
                    state TEXT,
                    PRIMARY KEY (constraint_id, xmin)
                )
            """)
            
            # 3. Nhật ký giao dịch phục vụ khôi phục sự cố phân tán (Transaction Logs)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Transaction_Log (
                    tx_id TEXT PRIMARY KEY,
                    state TEXT,                -- 'PREPARED', 'COMMITTED', 'ABORTED'
                    write_set TEXT,            -- Lưu trữ JSON của các thay đổi ghi
                    t_commit INTEGER           -- Nhãn thời gian commit thực tế
                )
            """)
            
            conn.commit()

    # ==========================================
    # CÁC TIỆN ÍCH CHO MVCC READ VISIBILITY
    # ==========================================
    
    def read_product(self, product_id: int, t_start: int) -> Optional[Dict]:
        """Đọc phiên bản sản phẩm hợp lệ tại thời điểm bắt đầu giao dịch (t_start)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Quy tắc MVCC Visibility:
            # 1. Phiên bản phải được COMMITTED.
            # 2. Được tạo trước hoặc bằng t_start (xmin <= t_start).
            # 3. Chưa bị xóa hoặc bị xóa sau t_start (xmax IS NULL hoặc xmax > t_start).
            cursor.execute("""
                SELECT product_id, product_name, stock_level, xmin, xmax, state
                FROM Product_Inventory
                WHERE product_id = ? 
                  AND state = 'COMMITTED'
                  AND xmin <= ?
                  AND (xmax IS NULL OR xmax > ?)
                ORDER BY xmin DESC
                LIMIT 1
            """, (product_id, t_start, t_start))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    def read_constraint(self, constraint_id: str, t_start: int) -> Optional[Dict]:
        """Đọc phiên bản ràng buộc hợp lệ tại t_start."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT constraint_id, last_tx_id, xmin, xmax, state
                FROM Inventory_Constraint
                WHERE constraint_id = ?
                  AND state = 'COMMITTED'
                  AND xmin <= ?
                  AND (xmax IS NULL OR xmax > ?)
                ORDER BY xmin DESC
                LIMIT 1
            """, (constraint_id, t_start, t_start))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None

    # ==========================================
    # PHASE 1: PREPARE & FIRST-COMMITTER-WINS
    # ==========================================
    
    def prepare_transaction(self, tx_id: str, t_start: int, writes: List[Dict], constraints: List[str]) -> Tuple[bool, str]:
        """
        Giai đoạn 1 của 2PC: Kiểm tra xung đột ghi (FCW) và ghi dữ liệu dưới trạng thái PREPARED.
        
        Quy trình xác thực FCW (First-Committer-Wins):
        1. Kiểm tra xem có phiên bản COMMITTED nào của sản phẩm mới hơn t_start không.
        2. Kiểm tra xem có giao dịch KHÁC đang ở trạng thái PREPARED trên cùng sản phẩm không
           (ngăn chặn race condition khi 2 giao dịch đồng thời cùng pass bước 1).
        3. Kiểm tra tương tự trên bảng ràng buộc dùng chung (Materializing Conflicts).
        
        Lưu ý về kiểu dữ liệu SQLite:
        - Các cột xmin/xmax được khai báo INTEGER nhưng SQLite sử dụng dynamic typing.
        - Khi PREPARED: xmin lưu tx_id (TEXT UUID) làm khóa tạm thời.
        - Khi COMMITTED (Phase 2): xmin được cập nhật thành t_commit (INTEGER) thực tế.
        - Điều này cho phép phân biệt rõ ràng phiên bản tạm thời (TEXT) vs. chính thức (INTEGER).
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # --- 1. KIỂM TRA XUNG ĐỘT GHI (First-Committer-Wins) ---
            for w in writes:
                p_id = w["product_id"]
                
                # 1a. Kiểm tra xem có phiên bản COMMITTED nào mới hơn t_start hay không
                cursor.execute("""
                    SELECT COUNT(*) as count FROM Product_Inventory
                    WHERE product_id = ?
                      AND state = 'COMMITTED'
                      AND xmin > ?
                """, (p_id, t_start))
                
                if cursor.fetchone()["count"] > 0:
                    return False, f"FCW Conflict: San pham {p_id} da bi cap nhat boi giao dich khac sau nhãn start_ts {t_start}"
                
                # 1b. Kiểm tra xem có giao dịch KHÁC đang PREPARED trên cùng sản phẩm không
                # (Ngăn chặn race condition: 2 giao dịch đồng thời cùng vượt qua bước 1a)
                cursor.execute("""
                    SELECT COUNT(*) as count FROM Product_Inventory
                    WHERE product_id = ?
                      AND state = 'PREPARED'
                      AND xmin != ?
                """, (p_id, tx_id))
                
                if cursor.fetchone()["count"] > 0:
                    return False, f"FCW Conflict: San pham {p_id} dang bi khoa boi giao dich khac (PREPARED)"
            
            # 2. Kiểm tra xung đột ghi trên bảng ràng buộc để ngăn chặn Write Skew
            for c_id in constraints:
                # 2a. Kiểm tra phiên bản COMMITTED mới hơn t_start
                cursor.execute("""
                    SELECT COUNT(*) as count FROM Inventory_Constraint
                    WHERE constraint_id = ?
                      AND state = 'COMMITTED'
                      AND xmin > ?
                """, (c_id, t_start))
                
                if cursor.fetchone()["count"] > 0:
                    return False, f"FCW Conflict: Rang buoc {c_id} da bi thay doi boi giao dich khac sau nhan start_ts {t_start}"
                
                # 2b. Kiểm tra giao dịch KHÁC đang PREPARED trên cùng ràng buộc
                cursor.execute("""
                    SELECT COUNT(*) as count FROM Inventory_Constraint
                    WHERE constraint_id = ?
                      AND state = 'PREPARED'
                      AND xmin != ?
                """, (c_id, tx_id))
                
                if cursor.fetchone()["count"] > 0:
                    return False, f"FCW Conflict: Rang buoc {c_id} dang bi khoa boi giao dich khac (PREPARED)"

            # --- 3. GHI BẢN GHI LÂM THỜI (Trạng thái PREPARED) ---
            # Ghi vào Transaction Log cục bộ để phục vụ Crash Recovery
            cursor.execute("""
                INSERT OR REPLACE INTO Transaction_Log (tx_id, state, write_set)
                VALUES (?, 'PREPARED', ?)
            """, (tx_id, json.dumps({"writes": writes, "constraints": constraints})))

            for w in writes:
                p_id = w["product_id"]
                p_name = w["product_name"]
                new_stock = w["stock_level"]
                
                # Cập nhật xmax của phiên bản COMMITTED hiện tại để tạm khóa phiên bản này.
                # Gán xmax = tx_id (TEXT) để đánh dấu phiên bản cũ đã bị thay thế bởi giao dịch tx_id.
                # SQLite chấp nhận dynamic typing nên TEXT trong cột INTEGER là hợp lệ.
                # Khi commit (Phase 2), xmax sẽ được cập nhật thành t_commit (INTEGER thực tế).
                cursor.execute("""
                    UPDATE Product_Inventory
                    SET xmax = ?
                    WHERE product_id = ?
                      AND state = 'COMMITTED'
                      AND xmax IS NULL
                """, (tx_id, p_id))
                
                # Thêm phiên bản mới với trạng thái PREPARED, xmin = tx_id (TEXT tạm thời)
                cursor.execute("""
                    INSERT INTO Product_Inventory (product_id, product_name, stock_level, xmin, xmax, state)
                    VALUES (?, ?, ?, ?, NULL, 'PREPARED')
                """, (p_id, p_name, new_stock, tx_id))

            # Thực hiện tương tự cho bảng ràng buộc dùng chung (Materializing Conflicts)
            for c_id in constraints:
                # Đánh dấu xmax của phiên bản ràng buộc cũ
                cursor.execute("""
                    UPDATE Inventory_Constraint
                    SET xmax = ?
                    WHERE constraint_id = ?
                      AND state = 'COMMITTED'
                      AND xmax IS NULL
                """, (tx_id, c_id))
                
                # Thêm bản ghi ràng buộc mới dưới trạng thái PREPARED
                cursor.execute("""
                    INSERT INTO Inventory_Constraint (constraint_id, last_tx_id, xmin, xmax, state)
                    VALUES (?, ?, ?, NULL, 'PREPARED')
                """, (c_id, tx_id, tx_id))

            conn.commit()
            return True, "PREPARED SUCCESS"
            
        except Exception as e:
            conn.rollback()
            return False, f"Storage Error during Prepare: {str(e)}"
        finally:
            conn.close()

    # ==========================================
    # PHASE 2: COMMIT / ABORT
    # ==========================================
    
    def commit_transaction(self, tx_id: str, t_commit: int) -> bool:
        """Giai đoạn 2 của 2PC: Nhận lệnh COMMIT, chính thức kích hoạt các phiên bản tạm thời."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Cập nhật Transaction Log
            cursor.execute("""
                UPDATE Transaction_Log
                SET state = 'COMMITTED', t_commit = ?
                WHERE tx_id = ?
            """, (t_commit, tx_id))
            
            # Chính thức chuyển xmax của phiên bản cũ sang T_commit thực tế
            cursor.execute("""
                UPDATE Product_Inventory
                SET xmax = ?
                WHERE xmax = ? AND state = 'COMMITTED'
            """, (t_commit, tx_id))
            
            # Chuyển phiên bản mới sang trạng thái COMMITTED và gán xmin = T_commit
            cursor.execute("""
                UPDATE Product_Inventory
                SET xmin = ?, state = 'COMMITTED'
                WHERE xmin = ? AND state = 'PREPARED'
            """, (t_commit, tx_id))

            # Thực hiện tương tự cho các ràng buộc chung
            cursor.execute("""
                UPDATE Inventory_Constraint
                SET xmax = ?
                WHERE xmax = ? AND state = 'COMMITTED'
            """, (t_commit, tx_id))
            
            cursor.execute("""
                UPDATE Inventory_Constraint
                SET xmin = ?, state = 'COMMITTED'
                WHERE xmin = ? AND state = 'PREPARED'
            """, (t_commit, tx_id))

            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            print(f"Error during commit on node {self.node_id}: {str(e)}")
            return False
        finally:
            conn.close()

    def abort_transaction(self, tx_id: str) -> bool:
        """Giai đoạn 2 của 2PC: Nhận lệnh ABORT, dọn dẹp các phiên bản tạm thời."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # Cập nhật Transaction Log
            cursor.execute("""
                UPDATE Transaction_Log
                SET state = 'ABORTED'
                WHERE tx_id = ?
            """, (tx_id,))
            
            # Hoàn nguyên xmax của phiên bản cũ bằng cách đặt lại thành NULL
            cursor.execute("""
                UPDATE Product_Inventory
                SET xmax = NULL
                WHERE xmax = ?
            """, (tx_id,))
            
            # Xóa bỏ hoàn toàn phiên bản tạm thời PREPARED
            cursor.execute("""
                DELETE FROM Product_Inventory
                WHERE xmin = ? AND state = 'PREPARED'
            """, (tx_id,))

            # Hoàn nguyên trên bảng ràng buộc phụ
            cursor.execute("""
                UPDATE Inventory_Constraint
                SET xmax = NULL
                WHERE xmax = ?
            """, (tx_id,))
            
            cursor.execute("""
                DELETE FROM Inventory_Constraint
                WHERE xmin = ? AND state = 'PREPARED'
            """, (tx_id,))

            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            print(f"Error during abort on node {self.node_id}: {str(e)}")
            return False
        finally:
            conn.close()

    # ==========================================
    # CÁC TIỆN ÍCH DÒNG CƠ SỞ (SEED DỮ LIỆU)
    # ==========================================
    
    def seed_initial_data(self, products: List[Tuple[int, str, int]]):
        """Khởi tạo kho hàng ban đầu (giao dịch khởi tạo xmin = 0)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for p_id, p_name, stock in products:
                # Xóa dữ liệu cũ nếu trùng
                cursor.execute("DELETE FROM Product_Inventory WHERE product_id = ?", (p_id,))
                
                # Chèn bản ghi COMMITTED ban đầu tại thời điểm t=0
                cursor.execute("""
                    INSERT INTO Product_Inventory (product_id, product_name, stock_level, xmin, xmax, state)
                    VALUES (?, ?, ?, 0, NULL, 'COMMITTED')
                """, (p_id, p_name, stock))
                
            conn.commit()

    def seed_initial_constraint(self, constraint_id: str):
        """Khởi tạo bản ghi ràng buộc dùng chung ban đầu (t=0)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM Inventory_Constraint WHERE constraint_id = ?", (constraint_id,))
            cursor.execute("""
                INSERT INTO Inventory_Constraint (constraint_id, last_tx_id, xmin, xmax, state)
                VALUES (?, 'INIT_SYSTEM', 0, NULL, 'COMMITTED')
            """, (constraint_id,))
            conn.commit()
            
    def get_all_committed_products(self) -> List[Dict]:
        """Lấy danh sách các sản phẩm đang có hiệu lực mới nhất (phục vụ in log kiểm thử)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT product_id, product_name, stock_level, xmin, xmax, state
                FROM Product_Inventory
                WHERE state = 'COMMITTED' AND xmax IS NULL
                ORDER BY product_id ASC
            """)
            return [dict(row) for row in cursor.fetchall()]

    def get_all_committed_constraints(self) -> List[Dict]:
        """Lấy danh sách ràng buộc đang có hiệu lực phục vụ in log."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT constraint_id, last_tx_id, xmin, xmax, state
                FROM Inventory_Constraint
                WHERE state = 'COMMITTED' AND xmax IS NULL
            """)
            return [dict(row) for row in cursor.fetchall()]
