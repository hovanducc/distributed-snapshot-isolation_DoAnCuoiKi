import asyncio
import json
import os
from typing import Dict, Optional

class CoordinatorState:
    """
    Lớp quản lý trạng thái toàn cục của Bộ điều phối (Global Coordinator).
    
    Chức năng:
    - Duy trì đồng hồ logic tăng dần (Logical Clock / Timestamp Generator).
    - Theo dõi nhật ký trạng thái giao dịch đang hoạt động (Transaction Log).
    - Ghi bền vững (Persistence) nhật ký giao dịch ra file JSON trên đĩa cứng
      để đảm bảo khả năng phục hồi khi Coordinator bị sập và khởi động lại.
    """

    PERSISTENCE_PATH = "data/coordinator_log.json"

    def __init__(self):
        self.logical_clock: int = 0
        self.active_transactions: Dict[str, Dict] = {}  # tx_id -> {t_start, state, t_commit}
        self.lock = asyncio.Lock()
        
        # Khôi phục trạng thái từ file persistence nếu tồn tại
        self._load_persisted_state()

    async def get_next_timestamp(self) -> int:
        """Cấp phát nhãn thời gian logic tăng dần toàn cục (Centralized TSG)."""
        async with self.lock:
            self.logical_clock += 1
            return self.logical_clock

    def register_transaction(self, tx_id: str, t_start: int):
        """Đăng ký một giao dịch mới vào nhật ký trạng thái."""
        self.active_transactions[tx_id] = {
            "t_start": t_start,
            "state": "RUNNING",
            "t_commit": None  # Sẽ được gán khi commit thành công
        }
        self._persist_state()

    def update_transaction_state(self, tx_id: str, new_state: str, t_commit: Optional[int] = None):
        """
        Cập nhật trạng thái giao dịch trong nhật ký.
        Trạng thái hợp lệ: 'RUNNING', 'COMMITTING', 'COMMITTED', 'ABORTED'.
        """
        if tx_id in self.active_transactions:
            self.active_transactions[tx_id]["state"] = new_state
            if t_commit is not None:
                self.active_transactions[tx_id]["t_commit"] = t_commit
            self._persist_state()

    def get_transaction(self, tx_id: str) -> Optional[Dict]:
        """Lấy thông tin giao dịch theo ID."""
        return self.active_transactions.get(tx_id)

    def is_valid_transaction(self, tx_id: str) -> bool:
        """Kiểm tra giao dịch có tồn tại và còn hoạt động không."""
        return tx_id in self.active_transactions

    def _persist_state(self):
        """
        Ghi bền vững nhật ký giao dịch ra file JSON trên đĩa cứng.
        Đảm bảo Coordinator có thể khôi phục lại trạng thái sau khi sập nguồn.
        """
        try:
            os.makedirs(os.path.dirname(self.PERSISTENCE_PATH), exist_ok=True)
            data = {
                "logical_clock": self.logical_clock,
                "transactions": self.active_transactions
            }
            with open(self.PERSISTENCE_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            # Log lỗi nhưng không dừng hệ thống vì persistence là best-effort
            print(f"[CoordinatorState] WARNING: Không thể ghi persistence file: {e}")

    def _load_persisted_state(self):
        """
        Đọc lại nhật ký giao dịch từ file JSON khi Coordinator khởi động lại.
        Khôi phục logical_clock và danh sách giao dịch đã được ghi nhận.
        """
        try:
            if os.path.exists(self.PERSISTENCE_PATH):
                with open(self.PERSISTENCE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.logical_clock = data.get("logical_clock", 0)
                    self.active_transactions = data.get("transactions", {})
                print(f"[CoordinatorState] Đã khôi phục trạng thái: clock={self.logical_clock}, "
                      f"{len(self.active_transactions)} giao dịch từ persistence file.")
        except Exception as e:
            print(f"[CoordinatorState] WARNING: Không thể đọc persistence file: {e}. Khởi tạo trạng thái mới.")
            self.logical_clock = 0
            self.active_transactions = {}
