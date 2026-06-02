import httpx
import json
from typing import Dict, List, Optional, Tuple

class DBClient:
    """
    Client SDK giả lập tương tác giao dịch phân tán (Begin, Read, Write, Commit).
    
    Sử dụng httpx.AsyncClient để đảm bảo tính đồng thời thực sự khi nhiều
    client chạy song song trong asyncio.gather() (phục vụ kiểm thử benchmark chính xác).
    
    Quy trình sử dụng:
    1. await client.begin()       -> Khởi tạo giao dịch, nhận tx_id và t_start
    2. await client.read(id)      -> Đọc sản phẩm từ snapshot (hoặc từ write-set cục bộ)
    3. client.write(id, name, qty) -> Ghi tạm vào Write-Set đệm (client-side)
    4. await client.commit()      -> Gửi Write-Set lên Coordinator để thực hiện 2PC
    """

    def __init__(self, coordinator_url: str = "http://127.0.0.1:8000"):
        self.coordinator_url = coordinator_url
        self.tx_id: Optional[str] = None
        self.t_start: Optional[int] = None
        self.write_set: Dict[int, Dict] = {}  # product_id -> {product_id, product_name, stock_level}
        self.constraints: List[str] = []      # Cac khoa rang buoc can kiem tra dummy update

    async def begin(self) -> str:
        """Khoi dong giao dich moi, nhan Tx_ID va start_ts tu Coordinator."""
        url = f"{self.coordinator_url}/transaction/begin"
        async with httpx.AsyncClient() as client:
            r = await client.post(url, timeout=5.0)
            if r.status_code == 200:
                data = r.json()
                self.tx_id = data["tx_id"]
                self.t_start = data["t_start"]
                self.write_set = {}
                self.constraints = []
                return self.tx_id
            else:
                raise Exception(f"Loi khi Begin Transaction: {r.text}")

    async def read(self, product_id: int) -> Tuple[Optional[int], Optional[str]]:
        """
        Đọc dữ liệu từ snapshot hoặc từ Write-Set đệm (Read Your Own Writes).
        Trả về: (stock_level, product_name)
        """
        if not self.tx_id:
            raise Exception("Chua co Giao dich nao dang chay. Vui long goi begin() truoc.")

        # 1. Kiem tra tinh nhat quan Read-Your-Own-Writes (neu da ghi trong giao dich nay, doc tu dem)
        if product_id in self.write_set:
            w = self.write_set[product_id]
            return w["stock_level"], w["product_name"]

        # 2. Doc phien ban snapshot tu Coordinator
        url = f"{self.coordinator_url}/transaction/read?product_id={product_id}&t_start={self.t_start}"
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(url, timeout=5.0)
                if r.status_code == 200:
                    data = r.json()
                    return data["stock_level"], data["product_name"]
                elif r.status_code == 404:
                    return None, None
                else:
                    raise Exception(f"Loi khi doc san pham {product_id}: {r.text}")
        except httpx.RequestError as e:
            raise Exception(f"Loi ket noi toi Coordinator: {str(e)}")

    def write(self, product_id: int, product_name: str, stock_level: int):
        """Ghi tam thay doi vao Write-Set dem cua Giao dich (client-side, khong gui mang)."""
        if not self.tx_id:
            raise Exception("Chua co Giao dich nao dang chay. Vui long goi begin() truoc.")
            
        self.write_set[product_id] = {
            "product_id": product_id,
            "product_name": product_name,
            "stock_level": stock_level
        }

    def add_constraint_validation(self, constraint_id: str):
        """
        Dang ky khoa rang buoc de thuc hien Dummy Update.
        Tu do cu the hoa xung dot (Materialize Conflicts) de chan Write Skew.
        """
        if constraint_id not in self.constraints:
            self.constraints.append(constraint_id)

    async def commit(self) -> Tuple[bool, str]:
        """
        Gửi yêu cầu Commit và danh sách Write-Set, Constraints lên Coordinator.
        Coordinator sẽ điều phối giao thức 2-Phase Commit với các Node.
        Tra ve: (success: bool, message: str)
        """
        if not self.tx_id:
            raise Exception("Chua co Giao dich nao dang chay. Vui long goi begin() truoc.")

        url = f"{self.coordinator_url}/transaction/commit"
        
        # Chuyen write_set tu dict sang list
        writes_list = list(self.write_set.values())
        
        payload = {
            "tx_id": self.tx_id,
            "t_start": self.t_start,
            "writes": writes_list,
            "constraints": self.constraints
        }
        
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(url, json=payload, timeout=10.0)
                if r.status_code == 200:
                    res_data = r.json()
                    success = res_data.get("success", False)
                    message = res_data.get("message", "")
                    
                    # Reset trang thai client khi hoàn tất
                    self.tx_id = None
                    self.t_start = None
                    self.write_set = {}
                    self.constraints = []
                    
                    return success, message
                else:
                    return False, f"Loi he thong Coordinator: {r.text}"
        except Exception as e:
            return False, f"Loi ket noi khi gui Commit: {str(e)}"
