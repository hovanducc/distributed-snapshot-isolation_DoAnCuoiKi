import json
import httpx
import sys
import os

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.utils.logger import setup_logger

logger = setup_logger("DatabaseSeeder")

def seed_databases():
    """Tự động seed dữ liệu mẫu ban đầu cho Store_Front và Back_Office Nodes."""
    try:
        with open("config/config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
            
        nodes = config["nodes"]
        
        # 1. Định nghĩa dữ liệu mẫu (Sản phẩm 1 - 10)
        # Store_Front (Site 1) lưu ID lẻ: 1, 3, 5, 7, 9
        store_front_products = [
            {"product_id": 1, "product_name": "Milk (Store)", "stock_level": 50},
            {"product_id": 3, "product_name": "Bread (Store)", "stock_level": 30},
            {"product_id": 5, "product_name": "Egg (Store)", "stock_level": 100},
            {"product_id": 7, "product_name": "Coffee (Store)", "stock_level": 40},
            {"product_id": 9, "product_name": "Tea (Store)", "stock_level": 25},
            # Sản phẩm 101 là sản phẩm đặc biệt dùng để test Write Skew (được lưu cả Store_Front và Back_Office)
            {"product_id": 101, "product_name": "Premium Apple (Store Front)", "stock_level": 6}
        ]
        
        # Back_Office (Site 2) lưu ID chẵn: 2, 4, 6, 8, 10
        back_office_products = [
            {"product_id": 2, "product_name": "Milk Bulk (Warehouse)", "stock_level": 500},
            {"product_id": 4, "product_name": "Bread Bulk (Warehouse)", "stock_level": 300},
            {"product_id": 6, "product_name": "Egg Bulk (Warehouse)", "stock_level": 1000},
            {"product_id": 8, "product_name": "Coffee Bulk (Warehouse)", "stock_level": 400},
            {"product_id": 10, "product_name": "Tea Bulk (Warehouse)", "stock_level": 250},
            # Sản phẩm 101 đặc biệt (lưu tại Back Office với số lượng 5, tổng 6+5=11 >= 10)
            {"product_id": 102, "product_name": "Premium Apple (Back Office)", "stock_level": 5}
        ]
        
        # Lưu ý: Trong kịch bản kiểm thử ràng buộc Write Skew, chúng ta sẽ định nghĩa một bản ghi kiểm thử chéo:
        # Cụ thể, Product 101 (Store Front) đại diện cho Stock ở Store và Product 102 (Back Office) đại diện cho Stock ở Back Office.
        # Ràng buộc toàn cục: Stock(101) + Stock(102) >= 10.
        # Điều này hoàn toàn phản ánh đúng yêu cầu của Đề tài 28: "Product_Inventory stored across two sites: Store_Front and Back_Office. The logical Stock Level constraint".
        
        # 2. Gửi request seed cho Site 1 (Store_Front)
        sf_url = f"{nodes['store_front']['url']}/seed"
        logger.info(f"Gui yeu cau seed du lieu toi Store Front: {sf_url}")
        
        # Khởi tạo bản ghi ràng buộc chéo dùng chung để Materialize Conflicts
        # Bản ghi ràng buộc là 'constraint_apple_total_stock' đại diện cho ràng buộc Stock(101) + Stock(102) >= 10
        sf_payload = {
            "products": store_front_products,
            "constraints": ["constraint_apple_total_stock"]
        }
        
        with httpx.Client(timeout=10.0) as client:
            r_sf = client.post(sf_url, json=sf_payload)
            if r_sf.status_code == 200:
                logger.info("Seed thanh cong Store Front DB!")
            else:
                logger.error(f"Seed that bai Store Front: {r_sf.text}")
                
        # 3. Gửi request seed cho Site 2 (Back_Office)
        bo_url = f"{nodes['back_office']['url']}/seed"
        logger.info(f"Gui yeu cau seed du lieu toi Back Office: {bo_url}")
        
        bo_payload = {
            "products": back_office_products,
            "constraints": ["constraint_apple_total_stock"]
        }
        
        with httpx.Client(timeout=10.0) as client:
            r_bo = client.post(bo_url, json=bo_payload)
            if r_bo.status_code == 200:
                logger.info("Seed thanh cong Back Office DB!")
            else:
                logger.error(f"Seed that bai Back Office: {r_bo.text}")
                
    except Exception as e:
        logger.error(f"Loi ket noi hoac goi API seed: {str(e)}")
        logger.error("Vui long dam bao ca hai Node Server dang hoat dong truoc khi chay seed_db.")

if __name__ == "__main__":
    seed_databases()
