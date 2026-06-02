import sys
import os
import json
import argparse
import asyncio
import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional

# Thêm thư mục gốc vào PYTHONPATH để có thể import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.nodes.storage import LocalStorage
from src.utils.logger import setup_logger

# 1. PHÂN TÍCH THAM SỐ KHỞI ĐỘNG
parser = argparse.ArgumentParser(description="Chay database node server cho do an SI.")
parser.add_argument("--node", type=str, required=True, choices=["store_front", "back_office"], help="Ten dinh danh node")
args, unknown = parser.parse_known_args()

NODE_ID = args.node

# 2. ĐỌC CẤU HÌNH HỆ THỐNG
with open("config/config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

NODE_CONFIG = config["nodes"][NODE_ID]
LATENCY_SECS = config["simulation"]["network_latency_seconds"]

# Khởi tạo logger riêng cho Node
logger = setup_logger(NODE_CONFIG["name"])
logger.info(f"Bat dau khoi dong Node Server: {NODE_CONFIG['name']} tren port {NODE_CONFIG['port']}")

# 3. KHỞI TẠO LOCAL STORAGE (SQLITE)
db_path = NODE_CONFIG["db_path"]
storage = LocalStorage(db_path, NODE_ID)

# 4. THIẾT LẬP FASTAPI APPLICATION
app = FastAPI(title=NODE_CONFIG["name"])

# Giả lập độ trễ mạng phân tán
async def inject_network_latency():
    if LATENCY_SECS > 0:
        await asyncio.sleep(LATENCY_SECS)

# Định nghĩa Pydantic Models cho request body
class ProductWriteModel(BaseModel):
    product_id: int
    product_name: str
    stock_level: int

class PrepareRequest(BaseModel):
    tx_id: str
    t_start: int
    writes: List[ProductWriteModel]
    constraints: List[str]

class CommitRequest(BaseModel):
    tx_id: str
    t_commit: int

class AbortRequest(BaseModel):
    tx_id: str

class SeedProductModel(BaseModel):
    product_id: int
    product_name: str
    stock_level: int

class SeedRequest(BaseModel):
    products: List[SeedProductModel]
    constraints: Optional[List[str]] = []

# ==========================================
# CƠ CHẾ TỰ ĐỘNG PHỤC HỒI SAU SỰ CỐ (CRASH RECOVERY)
# ==========================================

async def run_recovery_protocol():
    """
    Kiểm tra nhật ký giao dịch và hỏi Coordinator để tự động phục hồi các giao dịch PREPARED.
    
    Quy trình phục hồi (Recovery Protocol):
    1. Đọc các giao dịch đang ở trạng thái PREPARED từ Transaction_Log cục bộ trên SQLite.
    2. Kết nối đến Coordinator qua endpoint /debug/transactions để tra cứu quyết định cuối cùng.
    3. Nếu Coordinator đã COMMITTED: tiến hành commit cục bộ với t_commit thực tế.
    4. Nếu Coordinator đã ABORTED hoặc không tìm thấy: tiến hành abort cục bộ.
    """
    try:
        with storage._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tx_id FROM Transaction_Log WHERE state = 'PREPARED'")
            prepared_txs = [row["tx_id"] for row in cursor.fetchall()]
            
        if not prepared_txs:
            logger.info("RECOVERY: Khong co giao dich nao o trang thai PREPARED cần phuc hoi.")
            return
            
        logger.warning(f"RECOVERY: Phat hien {len(prepared_txs)} giao dich dang treo PREPARED. Bat dau ket noi hoi Coordinator...")
        
        coord_url = f"{config['coordinator']['url']}/debug/transactions"
        
        async with httpx.AsyncClient() as client:
            r = await client.get(coord_url, timeout=5.0)
            if r.status_code == 200:
                coord_data = r.json()
                active_txs = coord_data.get("transactions", {})
                
                for tx_id in prepared_txs:
                    if tx_id in active_txs:
                        coord_state = active_txs[tx_id].get("state")
                        if coord_state == "COMMITTED":
                            # Sử dụng t_commit thực tế từ Coordinator (thay vì logical_clock)
                            # để đảm bảo nhãn thời gian MVCC chính xác sau khi phục hồi
                            actual_t_commit = active_txs[tx_id].get("t_commit")
                            if actual_t_commit is not None:
                                storage.commit_transaction(tx_id, actual_t_commit)
                                logger.info(f"RECOVERY SUCCESS: Da phuc hoi COMMIT thành cong cho TX_ID: {tx_id} (commit_ts={actual_t_commit})")
                            else:
                                logger.error(f"RECOVERY ERROR: TX_ID {tx_id} COMMITTED nhung thieu t_commit. Chu dong ABORT cho an toan.")
                                storage.abort_transaction(tx_id)
                        elif coord_state == "ABORTED":
                            storage.abort_transaction(tx_id)
                            logger.info(f"RECOVERY SUCCESS: Da phuc hoi ABORT thanh cong cho TX_ID: {tx_id}")
                        else:
                            # Nếu Coordinator vẫn đang chạy nhưng chưa quyết định, quy tắc an toàn là abort
                            storage.abort_transaction(tx_id)
                            logger.info(f"RECOVERY: TX_ID: {tx_id} dang running -> Chu dong ABORT cho an toan.")
                    else:
                        # Giao dịch không tồn tại trên bộ nhớ active của Coordinator
                        storage.abort_transaction(tx_id)
                        logger.info(f"RECOVERY SUCCESS: Da phuc hoi ABORT (Do Coordinator khong chua phien lam viec) cho TX_ID: {tx_id}")
            else:
                logger.error(f"RECOVERY ERROR: Coordinator phan hoi loi HTTP {r.status_code}. Chua the phuc hoi.")
    except Exception as e:
        logger.error(f"RECOVERY ERROR: Gap su co ket noi den Coordinator khi phuc hoi: {str(e)}")

# Ghi chú: @app.on_event("startup") vẫn hoạt động trên các phiên bản FastAPI phổ biến.
# Đối với FastAPI >= 0.109, có thể chuyển sang sử dụng lifespan context manager.
@app.on_event("startup")
async def startup_event():
    """Khi máy chủ khởi động lại, kích hoạt giao thức phục hồi tự động."""
    logger.info("SYSTEM: Node dang chay chuong trinh khoi dong...")
    # Chạy giao thức phục hồi bất đồng bộ để tránh chặn tiến trình chính
    asyncio.create_task(run_recovery_protocol())

# ==========================================
# CÁC API ENDPOINTS
# ==========================================

@app.get("/products/{product_id}")
async def get_product(product_id: int, t_start: int):
    """Đọc sản phẩm dựa trên nhãn đọc t_start (MVCC Read)."""
    await inject_network_latency()
    
    # Kiểm tra xem sản phẩm có thuộc phạm vi phân mảnh của node này không
    min_id = NODE_CONFIG["product_id_range"]["start"]
    max_id = NODE_CONFIG["product_id_range"]["end"]
    range_type = NODE_CONFIG["product_id_range"]["type"]
    
    # Kiểm tra tính hợp lệ của mảnh ngang
    is_valid_range = (min_id <= product_id <= max_id)
    is_valid_type = (range_type == "odd" and product_id % 2 != 0) or (range_type == "even" and product_id % 2 == 0)
    
    if not (is_valid_range and is_valid_type):
        logger.warning(f"Sản phẩm {product_id} không thuộc phân mảnh ngang của Node {NODE_ID}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"Sản phẩm {product_id} không thuộc phân mảnh ngang của {NODE_ID}"
        )
        
    product = storage.read_product(product_id, t_start)
    if not product:
        logger.warning(f"Đọc MVCC: Khong tim thay san pham {product_id} hoac phien ban hop le tai start_ts={t_start}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="San pham khong ton tai")
        
    logger.info(f"Đọc MVCC: Doc thanh cong san pham {product_id} (Ton kho: {product['stock_level']}) tai start_ts={t_start}")
    return product

@app.get("/constraints/{constraint_id}")
async def get_constraint(constraint_id: str, t_start: int):
    """Đọc bản ghi ràng buộc dựa trên nhãn t_start."""
    await inject_network_latency()
    constraint = storage.read_constraint(constraint_id, t_start)
    if not constraint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rang buoc khong ton tai")
    return constraint

@app.post("/prepare")
async def prepare_transaction(req: PrepareRequest):
    """Phase 1: Validate FCW và ghi dữ liệu tạm thời (PREPARED)."""
    await inject_network_latency()
    logger.info(f"2PC_PREPARE: Nhận yêu cầu chuẩn bị cho TX_ID: {req.tx_id} (start_ts={req.t_start})")
    
    # Chuyển đổi dữ liệu sang dạng storage hiểu
    writes_list = [{"product_id": w.product_id, "product_name": w.product_name, "stock_level": w.stock_level} for w in req.writes]
    
    success, message = storage.prepare_transaction(req.tx_id, req.t_start, writes_list, req.constraints)
    
    if success:
        logger.info(f"2PC_PREPARE: TX_ID: {req.tx_id} kiem tra xung dot hop le -> Trang thai: PREPARED")
        return {"success": True, "message": message}
    else:
        logger.error(f"2PC_PREPARE: TX_ID: {req.tx_id} that bai do {message} -> Trang thai: ABORTED")
        return {"success": False, "message": message}

@app.post("/commit")
async def commit_transaction(req: CommitRequest):
    """Phase 2: Chuyển dữ liệu sang trạng thái COMMITTED với nhãn thời gian t_commit."""
    await inject_network_latency()
    logger.info(f"2PC_COMMIT: Nhận lệnh CAM KẾT cho TX_ID: {req.tx_id} (commit_ts={req.t_commit})")
    
    success = storage.commit_transaction(req.tx_id, req.t_commit)
    if success:
        logger.info(f"2PC_COMMIT: TX_ID: {req.tx_id} da CAM KẾT vinh vien tren o cung.")
        return {"success": True}
    else:
        logger.error(f"2PC_COMMIT: Ghi nhap ky commit TX_ID: {req.tx_id} that bai!")
        return {"success": False}

@app.post("/abort")
async def abort_transaction(req: AbortRequest):
    """Phase 2: Hủy bỏ các phiên bản tạm thời (ABORTED)."""
    await inject_network_latency()
    logger.warning(f"2PC_ABORT: Nhận lệnh HỦY GIAO DỊCH cho TX_ID: {req.tx_id}")
    
    success = storage.abort_transaction(req.tx_id)
    if success:
        logger.info(f"2PC_ABORT: TX_ID: {req.tx_id} da don dep va huy cac phien ban tam thoi.")
        return {"success": True}
    else:
        return {"success": False}

@app.post("/seed")
async def seed_data(req: SeedRequest):
    """Endpoint đặc biệt phục vụ seed kho hàng ban đầu khi khoi dong de tai."""
    await inject_network_latency()
    logger.info(f"SYSTEM: Dang seed du lieu kho ban dau tai {NODE_CONFIG['name']}...")
    
    products_list = [(p.product_id, p.product_name, p.stock_level) for p in req.products]
    storage.seed_initial_data(products_list)
    
    for c_id in req.constraints:
        storage.seed_initial_constraint(c_id)
        
    logger.info(f"SYSTEM: Seed du lieu hoan tat tren {NODE_CONFIG['name']}!")
    return {"success": True}

# ==========================================
# CÁC ENDPOINT DEBUG & HIỂN THỊ LOGS
# ==========================================

@app.get("/debug/products")
async def debug_products():
    """Lấy toàn bộ các sản phẩm đang có hiệu lực phục vụ in trace log."""
    return storage.get_all_committed_products()

@app.get("/debug/constraints")
async def debug_constraints():
    """Lấy toàn bộ cac rang buoc dang co hieu luc."""
    return storage.get_all_committed_constraints()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=NODE_CONFIG["host"], port=NODE_CONFIG["port"])
