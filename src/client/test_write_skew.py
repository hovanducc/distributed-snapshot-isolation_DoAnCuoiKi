import asyncio
import sys
import os
import httpx

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.client.client import DBClient
from src.utils.logger import setup_logger

logger = setup_logger("WriteSkewTest")

async def reset_write_skew_database():
    """Khởi tạo trạng thái ban đầu cho kịch bản Write Skew: Product 101 = 6, Product 102 = 5 (Tổng 11 >= 10)."""
    logger.info("SYSTEM: Resetting database for Write Skew Scenario...")
    
    # Store Front có sản phẩm lẻ 101 (stock = 6) và đăng ký ràng buộc 'constraint_apple_total_stock'
    seed_sf_url = "http://127.0.0.1:8001/seed"
    sf_payload = {
        "products": [{"product_id": 101, "product_name": "Premium Apple (Store Front)", "stock_level": 6}],
        "constraints": ["constraint_apple_total_stock"]
    }
    
    # Back Office có sản phẩm chẵn 102 (stock = 5) và đăng ký ràng buộc 'constraint_apple_total_stock'
    seed_bo_url = "http://127.0.0.1:8002/seed"
    bo_payload = {
        "products": [{"product_id": 102, "product_name": "Premium Apple (Back Office)", "stock_level": 5}],
        "constraints": ["constraint_apple_total_stock"]
    }
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(seed_sf_url, json=sf_payload, timeout=5.0)
            await client.post(seed_bo_url, json=bo_payload, timeout=5.0)
        logger.info("SYSTEM: Database successfully seeded for Write Skew Scenario!")
    except Exception as e:
        logger.error(f"Failed to reset DB: {str(e)}. Make sure both node servers are running.")

async def print_current_db_state(scenario_title: str):
    """In ra trạng thái tồn kho thực tế hiện tại của cả 2 Site."""
    try:
        async with httpx.AsyncClient() as client:
            r_sf = await client.get("http://127.0.0.1:8001/debug/products")
            r_bo = await client.get("http://127.0.0.1:8002/debug/products")
            
            sf_products = r_sf.json()
            bo_products = r_bo.json()
            
            stock_101 = next((p["stock_level"] for p in sf_products if p["product_id"] == 101), 0)
            stock_102 = next((p["stock_level"] for p in bo_products if p["product_id"] == 102), 0)
            
            total = stock_101 + stock_102
            logger.info(f"--- TRẠNG THÁI CSDL ({scenario_title}) ---")
            logger.info(f" -> Site 1 Store Front: San pham 101 (Tồn: {stock_101})")
            logger.info(f" -> Site 2 Back Office: San pham 102 (Tồn: {stock_102})")
            logger.info(f" => TỔNG TỒN KHO THỰC TẾ: {total} (Ràng buộc: Tổng >= 10)")
            if total < 10:
                logger.error(" !!! VI PHẠM RÀNG BUỘC TOÀN CỤC (TỔNG < 10) -> XẢY RA WRITE SKEW !!!")
            else:
                logger.info(" OK: Ràng buộc nhất quán toàn cục được bảo toàn (Tổng >= 10).")
            logger.info("--------------------------------------------------")
    except Exception as e:
        logger.error(f"Không thể đọc trạng thái debug DB: {str(e)}")

# ==========================================
# KỊCH BẢN 1: WRITE SKEW KHÔNG PHÒNG NGỪA
# ==========================================

async def run_scenario_without_protection():
    """
    Kịch bản 1: Hai giao dịch đồng thời rút hàng không bật Materializing Conflicts.
    SI thông thường cho phép cả 2 commit vì T1 sửa Product 101 và T2 sửa Product 102.
    -> Kỳ vọng: Cả 2 commit thành công, tổng tồn kho bị vi phạm (< 10).
    """
    logger.info("=" * 60)
    logger.info("=== KỊCH BẢN 1: GIẢ LẬP WRITE SKEW ANOMALY (SI THUẦN TÚY) ===")
    logger.info("=" * 60)
    await reset_write_skew_database()
    await print_current_db_state("Ban dau")
    
    # Khởi tạo hai DB Client đại diện cho 2 giao dịch đồng thời
    t1 = DBClient()
    t2 = DBClient()
    
    # 1. Khởi động T1 và T2 cục bộ
    await t1.begin()
    await t2.begin()
    logger.info(f" Giao dich T1 bat dau (start_ts={t1.t_start})")
    logger.info(f" Giao dich T2 bat dau (start_ts={t2.t_start})")
    
    # 2. T1 đọc tồn kho cả hai site để kiểm tra ràng buộc chéo
    stock_101_t1, _ = await t1.read(101)
    stock_102_t1, _ = await t1.read(102)
    total_t1 = stock_101_t1 + stock_102_t1
    logger.info(f" T1 đọc: San pham 101={stock_101_t1}, San pham 102={stock_102_t1} (Tong={total_t1})")
    
    # 3. T2 cũng đọc đồng thời
    stock_101_t2, _ = await t2.read(101)
    stock_102_t2, _ = await t2.read(102)
    total_t2 = stock_101_t2 + stock_102_t2
    logger.info(f" T2 đọc: San pham 101={stock_101_t2}, San pham 102={stock_102_t2} (Tong={total_t2})")
    
    # 4. T1 quyết định rút 1 đơn vị ở Store Front (Product 101: 6 -> 5).
    # Tổng sau rút: 5 + 5 = 10 >= 10 -> Hợp lệ dưới góc nhìn của T1.
    logger.info(" T1 chuẩn bị ghi: San pham 101 = 5")
    t1.write(101, "Premium Apple (Store Front)", 5)
    
    # 5. T2 quyết định rút 1 đơn vị ở Back Office (Product 102: 5 -> 4).
    # Tổng sau rút: 6 + 4 = 10 >= 10 -> Hợp lệ dưới góc nhìn của T2.
    logger.info(" T2 chuẩn bị ghi: San pham 102 = 4")
    t2.write(102, "Premium Apple (Back Office)", 4)
    
    # Giả lập trễ nhẹ để mô phỏng tương tác song song
    await asyncio.sleep(0.1)
    
    # 6. T1 tiến hành Commit trước
    logger.info(" T1 yeu cau COMMIT...")
    t1_success, t1_msg = await t1.commit()
    logger.info(f" T1 COMMIT: {'THANH CONG' if t1_success else 'THAT BAI (' + t1_msg + ')'}")
    
    # 7. T2 tiến hành Commit sau
    logger.info(" T2 yeu cau COMMIT...")
    t2_success, t2_msg = await t2.commit()
    logger.info(f" T2 COMMIT: {'THANH CONG' if t2_success else 'THAT BAI (' + t2_msg + ')'}")
    
    # 8. Xem kết quả
    await asyncio.sleep(0.5)
    await print_current_db_state("Sau khi ca hai giao dich commit")
    
    # Kết luận kịch bản
    if t1_success and t2_success:
        logger.warning(" >>> KẾT LUẬN: SI thuần túy KHÔNG PHÁT HIỆN Write Skew vì T1 và T2 ghi vào 2 bản ghi KHÁC NHAU trên 2 site khác nhau.")
    else:
        logger.info(" >>> KẾT LUẬN: Có giao dịch bị abort (có thể do race condition).")

# ==========================================
# KỊCH BẢN 2: CHẶN WRITE SKEW BẰNG DUMMY UPDATE
# ==========================================

async def run_scenario_with_protection():
    """
    Kịch bản 2: Bật Materializing Conflicts bằng cách đăng ký khóa ràng buộc chung.
    T2 sẽ bị ABORT vì T1 đã thực hiện ghi thành công và cập nhật khóa ràng buộc trước.
    -> Kỳ vọng: T1 commit thành công, T2 bị abort do FCW trên khóa ràng buộc.
    """
    logger.info("=" * 60)
    logger.info("=== KỊCH BẢN 2: CHẶN WRITE SKEW BẰNG MATERIALIZING CONFLICTS ===")
    logger.info("=" * 60)
    await reset_write_skew_database()
    await print_current_db_state("Ban dau")
    
    t1 = DBClient()
    t2 = DBClient()
    
    await t1.begin()
    await t2.begin()
    logger.info(f" Giao dich T1 bat dau (start_ts={t1.t_start})")
    logger.info(f" Giao dich T2 bat dau (start_ts={t2.t_start})")
    
    # Đăng ký khóa ràng buộc chung để ép buộc Dummy Update (Materializing Conflicts)
    t1.add_constraint_validation("constraint_apple_total_stock")
    t2.add_constraint_validation("constraint_apple_total_stock")
    logger.info(" Cả T1 và T2 đều đăng ký ràng buộc 'constraint_apple_total_stock' (Materializing Conflicts)")
    
    # Đọc dữ liệu
    stock_101_t1, _ = await t1.read(101)
    stock_102_t1, _ = await t1.read(102)
    logger.info(f" T1 đọc: San pham 101={stock_101_t1}, San pham 102={stock_102_t1} (Tong={stock_101_t1 + stock_102_t1})")
    
    stock_101_t2, _ = await t2.read(101)
    stock_102_t2, _ = await t2.read(102)
    logger.info(f" T2 đọc: San pham 101={stock_101_t2}, San pham 102={stock_102_t2} (Tong={stock_101_t2 + stock_102_t2})")
    
    # Chuẩn bị ghi
    logger.info(" T1 chuẩn bị ghi: San pham 101 = 5 (Đồng thời tự động Dummy Update lên constraint_apple_total_stock)")
    t1.write(101, "Premium Apple (Store Front)", 5)
    
    logger.info(" T2 chuẩn bị ghi: San pham 102 = 4 (Đồng thời tự động Dummy Update lên constraint_apple_total_stock)")
    t2.write(102, "Premium Apple (Back Office)", 4)
    
    await asyncio.sleep(0.1)
    
    # T1 Commit trước (First Committer)
    logger.info(" T1 yeu cau COMMIT...")
    t1_success, t1_msg = await t1.commit()
    logger.info(f" T1 COMMIT: {'THANH CONG' if t1_success else 'THAT BAI (' + t1_msg + ')'}")
    
    # T2 Commit sau (Second Committer)
    logger.info(" T2 yeu cau COMMIT...")
    t2_success, t2_msg = await t2.commit()
    if t2_success:
        logger.error(" T2 COMMIT THANH CONG -> THAT BAI! He thong da vi pham rang buoc.")
    else:
        logger.info(f" T2 COMMIT: THAT BAI. Thong diep loi: {t2_msg}")
        logger.info(" >>> CHẶN WRITE SKEW THÀNH CÔNG! Giao dịch thứ hai đã bị hủy để bảo vệ CSDL.")
        
    await asyncio.sleep(0.5)
    await print_current_db_state("Ket qua sau cung cua kịch bản 2")
    
    # Kết luận kịch bản
    if t1_success and not t2_success:
        logger.info(" >>> KẾT LUẬN: Cơ chế Materializing Conflicts hoạt động chính xác!")
        logger.info("     T1 commit thành công (First-Committer-Wins).")
        logger.info("     T2 bị abort vì phát hiện xung đột trên khóa ràng buộc dùng chung.")
        logger.info("     Tính nhất quán ràng buộc toàn cục (Tổng >= 10) được bảo toàn tuyệt đối.")

async def main():
    # Chạy lần lượt 2 kịch bản
    await run_scenario_without_protection()
    print("\n" + "=" * 60 + "\n")
    await run_scenario_with_protection()

if __name__ == "__main__":
    asyncio.run(main())
