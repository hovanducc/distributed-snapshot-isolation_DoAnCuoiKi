import sys
import os
import json
import httpx
import uuid
import asyncio
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Dict, List, Optional

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.utils.logger import setup_logger
from src.coordinator.state import CoordinatorState

# 1. ĐỌC CẤU HÌNH HỆ THỐNG
with open("config/config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

COORD_CONFIG = config["coordinator"]
NODES_CONFIG = config["nodes"]

logger = setup_logger("GlobalCoordinator")

# 2. KHỞI TẠO FASTAPI APPLICATION
app = FastAPI(title="Global Transaction Coordinator")

# Khởi tạo bộ quản lý trạng thái giao dịch (có persistence JSON trên đĩa cứng)
state = CoordinatorState()

# 3. ĐỊNH NGHĨA PYDANTIC MODELS
class TransactionBeginResponse(BaseModel):
    tx_id: str
    t_start: int

class ProductWriteModel(BaseModel):
    product_id: int
    product_name: str
    stock_level: int

class TransactionCommitRequest(BaseModel):
    tx_id: str
    t_start: int
    writes: List[ProductWriteModel]
    constraints: List[str]

# ==========================================
# CÁC API ENDPOINTS
# ==========================================

@app.post("/transaction/begin", response_model=TransactionBeginResponse)
async def begin_transaction():
    """Khởi tạo một giao dịch mới: Cấp phát Tx_ID và nhãn thời gian đọc t_start."""
    t_start = await state.get_next_timestamp()
    tx_id = str(uuid.uuid4())
    
    # Đăng ký giao dịch vào nhật ký trạng thái (có persistence)
    state.register_transaction(tx_id, t_start)
    
    logger.info(f"TX_BEGIN: Giao dich {tx_id} khoi dong voi nhan doc start_ts={t_start}")
    return {"tx_id": tx_id, "t_start": t_start}

@app.get("/transaction/read")
async def read_product(product_id: int, t_start: int):
    """
    Định tuyến yêu cầu đọc dữ liệu đến đúng Node chứa phân mảnh ngang tương ứng.
    (Giúp Client hoàn toàn trong suốt về vị trí phân mảnh dữ liệu - Location Transparency).
    """
    # 1. Định tuyến xem product_id thuộc về site nào
    target_node = None
    for node_key, node_conf in NODES_CONFIG.items():
        min_id = node_conf["product_id_range"]["start"]
        max_id = node_conf["product_id_range"]["end"]
        range_type = node_conf["product_id_range"]["type"]
        
        is_in_range = (min_id <= product_id <= max_id)
        is_type_match = (range_type == "odd" and product_id % 2 != 0) or (range_type == "even" and product_id % 2 == 0)
        
        if is_in_range and is_type_match:
            target_node = node_conf
            break
            
    if not target_node:
        logger.error(f"TX_READ: San pham ID {product_id} khong khop voi bat ky phan manh ngang nao trong he thong!")
        raise HTTPException(status_code=400, detail="San pham khong thuoc phan manh nao")

    # 2. Gọi API node tương ứng để đọc dữ liệu MVCC
    logger.info(f"TX_READ: Dinh tuyen doc san pham {product_id} toi {target_node['name']}")
    node_url = f"{target_node['url']}/products/{product_id}?t_start={t_start}"
    
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(node_url, timeout=5.0)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                raise HTTPException(status_code=404, detail="San pham khong ton tai")
            else:
                raise HTTPException(status_code=r.status_code, detail=r.text)
        except httpx.RequestError as e:
            logger.error(f"TX_READ: That bai khi ket noi toi {target_node['name']}: {str(e)}")
            raise HTTPException(status_code=503, detail=f"Nút {target_node['name']} khong phan hoi")

@app.post("/transaction/commit")
async def commit_transaction(req: TransactionCommitRequest):
    """
    Điều phối Giao thức Cam kết 2 Pha (2-Phase Commit - 2PC) toàn cục
    kết hợp xác thực xung đột First-Committer-Wins.
    """
    tx_id = req.tx_id
    t_start = req.t_start
    writes = req.writes
    constraints = req.constraints
    
    logger.info(f"TX_COMMIT: Bat dau dieu phoi 2PC cho Giao dich {tx_id}...")
    
    if not state.is_valid_transaction(tx_id):
        logger.error(f"TX_COMMIT: TX_ID {tx_id} khong ton tai hoac da bi huy.")
        raise HTTPException(status_code=400, detail="Giao dich khong hop le")
        
    state.update_transaction_state(tx_id, "COMMITTING")

    # 1. PHÂN CHIA WRITE-SET CHO TỪNG NODE PHÂN MẢNH
    node_writes = {node_key: [] for node_key in NODES_CONFIG.keys()}
    
    for w in writes:
        # Xác định nút đích của sản phẩm ghi
        target_node_id = None
        for node_key, node_conf in NODES_CONFIG.items():
            min_id = node_conf["product_id_range"]["start"]
            max_id = node_conf["product_id_range"]["end"]
            range_type = node_conf["product_id_range"]["type"]
            
            if min_id <= w.product_id <= max_id and (
                (range_type == "odd" and w.product_id % 2 != 0) or 
                (range_type == "even" and w.product_id % 2 == 0)
            ):
                target_node_id = node_key
                break
                
        if target_node_id:
            node_writes[target_node_id].append({
                "product_id": w.product_id,
                "product_name": w.product_name,
                "stock_level": w.stock_level
            })
            
    # Xác định các nút tham gia (có ghi du lieu hoac phai kiem tra rang buoc)
    # Ràng buộc dùng chung để Materialize Conflicts cần gửi tới các site tham gia
    # Trong kịch bản của chúng ta, ràng buộc 'constraint_apple_total_stock' được lưu trữ
    # trên cả hai site Store_Front và Back_Office. Do đó, cả hai site đều phai tham gia Prepare
    involved_nodes = set()
    for node_key, writes_list in node_writes.items():
        if len(writes_list) > 0:
            involved_nodes.add(node_key)
            
    # Bổ sung các site có ràng buộc phụ cần kiểm tra
    if len(constraints) > 0:
        # Trong đề tài này, ràng buộc chéo liên quan đến cả 2 site
        involved_nodes.add("store_front")
        involved_nodes.add("back_office")

    # Trường hợp giao dịch chỉ đọc (Read-only transaction)
    if not involved_nodes:
        state.update_transaction_state(tx_id, "COMMITTED")
        logger.info(f"TX_COMMIT: TX_ID {tx_id} la giao dich chỉ đọc -> Tu dong COMMITTED.")
        return {"success": True, "message": "Read-only transaction committed automatically."}

    # ==========================================
    # 2PC - PHASE 1: PREPARE
    # ==========================================
    logger.info(f"2PC_PHASE1: Gui yeu cau PREPARE toi cac nút tham gia: {list(involved_nodes)}")
    
    prepare_tasks = []
    
    async def prepare_node(node_key: str) -> bool:
        node_url = f"{NODES_CONFIG[node_key]['url']}/prepare"
        payload = {
            "tx_id": tx_id,
            "t_start": t_start,
            "writes": node_writes[node_key],
            "constraints": constraints # Gửi danh sách ràng buộc cần check
        }
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(node_url, json=payload, timeout=5.0)
                if r.status_code == 200:
                    res_json = r.json()
                    if res_json.get("success"):
                        logger.info(f"2PC_PHASE1: Nut {node_key} phan hoi READY")
                        return True
                    else:
                        logger.error(f"2PC_PHASE1: Nut {node_key} phan hoi REFUSE (Xung dot FCW): {res_json.get('message')}")
                        return False
                else:
                    logger.error(f"2PC_PHASE1: Nut {node_key} loi HTTP {r.status_code}")
                    return False
        except Exception as e:
            logger.error(f"2PC_PHASE1: Nut {node_key} gap su co ket noi: {str(e)}")
            return False

    # Chạy song song Prepare trên tất cả các nút
    prepare_results = await asyncio.gather(*(prepare_node(n) for n in involved_nodes))
    all_prepared = all(prepare_results)

    # ==========================================
    # 2PC - PHASE 2: COMMIT / ABORT
    # ==========================================
    if all_prepared:
        # TẤT CẢ READY -> TIẾN HÀNH COMMIT
        t_commit = await state.get_next_timestamp()
        logger.info(f"2PC_PHASE2: Tat ca các nút deu READY. Cap phat commit_ts={t_commit}. Bat dau phat lenh COMMIT...")
        
        async def commit_node(node_key: str):
            node_url = f"{NODES_CONFIG[node_key]['url']}/commit"
            try:
                async with httpx.AsyncClient() as client:
                    await client.post(node_url, json={"tx_id": tx_id, "t_commit": t_commit}, timeout=5.0)
            except Exception as e:
                # Giao thức 2PC quy định khi đã ra quyết định Commit thì phải lưu log và retry đến khi thành công
                logger.error(f"2PC_PHASE2: Gặp loi khi commit tren nut {node_key}: {str(e)}. (Sẽ can co che retry/phuc hoi)")

        await asyncio.gather(*(commit_node(n) for n in involved_nodes))
        
        # Lưu t_commit vào nhật ký giao dịch để phục vụ Crash Recovery trên các Node
        state.update_transaction_state(tx_id, "COMMITTED", t_commit=t_commit)
        logger.info(f"TX_COMMIT: Giao dich {tx_id} da CAM KẾT THANH CONG tai commit_ts={t_commit}!")
        return {"success": True, "t_commit": t_commit, "message": "Transaction committed successfully."}
        
    else:
        # CÓ NÚT THẤT BẠI/XUNG ĐỘT -> TIẾN HÀNH ABORT
        logger.warning(f"2PC_PHASE2: Co nut tu choi hoac loi ket noi. Phat lenh ABORT de huy bo giao dich {tx_id}...")
        
        async def abort_node(node_key: str):
            node_url = f"{NODES_CONFIG[node_key]['url']}/abort"
            try:
                async with httpx.AsyncClient() as client:
                    await client.post(node_url, json={"tx_id": tx_id}, timeout=5.0)
            except Exception as e:
                logger.error(f"2PC_PHASE2: Gap loi khi abort tren nut {node_key}: {str(e)}")

        await asyncio.gather(*(abort_node(n) for n in involved_nodes))
        
        state.update_transaction_state(tx_id, "ABORTED")
        logger.warning(f"TX_COMMIT: Giao dich {tx_id} da bi HUY (ABORTED) thanh cong!")
        return {"success": False, "message": "Transaction aborted due to write conflicts or network issues."}

# ==========================================
# ENDPOINT DEBUG & HIỂN THỊ TRẠNG THÁI
# ==========================================

@app.get("/debug/transactions")
async def debug_transactions():
    """
    Trả về trạng thái toàn cục: đồng hồ logic và danh sách giao dịch đang hoạt động.
    Endpoint này được sử dụng bởi các Node trong quá trình Crash Recovery
    để tra cứu quyết định cuối cùng (COMMITTED/ABORTED) và giá trị t_commit thực tế.
    """
    return {
        "logical_clock": state.logical_clock,
        "transactions": state.active_transactions
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=COORD_CONFIG["host"], port=COORD_CONFIG["port"])
