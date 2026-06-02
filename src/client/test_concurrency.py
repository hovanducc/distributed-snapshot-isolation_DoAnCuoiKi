import asyncio
import time
import csv
import os
import sys
import matplotlib
matplotlib.use('Agg')  # Backend không cần GUI, phù hợp để xuất file ảnh tự động
import matplotlib.pyplot as plt
import httpx

# Thêm thư mục gốc vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.client.client import DBClient
from src.utils.logger import setup_logger

logger = setup_logger("ConcurrencyBenchmark")

# Đường dẫn tệp đầu ra
OUTPUT_DIR = "output"
CSV_PATH = os.path.join(OUTPUT_DIR, "benchmark_results.csv")
ABORT_CHART_PATH = os.path.join(OUTPUT_DIR, "abort_rate_chart.png")
TPS_CHART_PATH = os.path.join(OUTPUT_DIR, "tps_chart.png")
LATENCY_CHART_PATH = os.path.join(OUTPUT_DIR, "latency_chart.png")

# Đảm bảo thư mục đầu ra tồn tại
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Gọi API để reset CSDL trước mỗi benchmark bằng seed
async def reset_database():
    """Reset dữ liệu cục bộ trên Store Front để thực hiện benchmark độc lập."""
    logger.info("SYSTEM: Resetting database for clean benchmark...")
    
    # Reset Store Front DB
    seed_sf_url = "http://127.0.0.1:8001/seed"
    sf_payload = {
        "products": [
            {"product_id": 1, "product_name": "Milk (Store)", "stock_level": 100}
        ],
        "constraints": []
    }
    
    try:
        async with httpx.AsyncClient() as client:
            await client.post(seed_sf_url, json=sf_payload, timeout=5.0)
    except Exception as e:
        logger.error(f"Failed to reset DB: {str(e)}. Make sure nodes are running.")

async def simulate_single_client(client_id: int, product_id: int) -> dict:
    """Giả lập một client thực hiện đọc và ghi đồng thời lên 1 sản phẩm."""
    client = DBClient()
    start_time = time.time()
    
    try:
        # 1. Begin Transaction (async)
        await client.begin()
        
        # 2. Read Product (Đọc tồn kho từ snapshot - async)
        stock, name = await client.read(product_id)
        if stock is None:
            return {"client_id": client_id, "success": False, "reason": "Not Found", "latency": time.time() - start_time}
            
        # 3. Write Product (Trừ tồn kho đi 1 - client-side, không cần async)
        new_stock = stock - 1
        client.write(product_id, name, new_stock)
        
        # Giả lập thời gian xử lý nghiệp vụ của client (50ms)
        await asyncio.sleep(0.05)
        
        # 4. Commit Transaction (async - kích hoạt 2PC)
        success, message = await client.commit()
        latency = time.time() - start_time
        
        return {
            "client_id": client_id,
            "success": success,
            "reason": message if not success else "COMMITTED",
            "latency": latency
        }
        
    except Exception as e:
        latency = time.time() - start_time
        return {
            "client_id": client_id,
            "success": False,
            "reason": str(e),
            "latency": latency
        }

async def run_benchmark_for_concurrency(n: int, product_id: int) -> dict:
    """Chạy đồng thời N clients và đo lường số liệu."""
    await reset_database()
    logger.info(f"BENCHMARK: Running test with N = {n} concurrent clients...")
    
    start_time = time.time()
    
    # Tạo danh sách các task chạy đồng thời (thực sự song song nhờ async client)
    tasks = [simulate_single_client(i, product_id) for i in range(n)]
    
    # Chạy đồng thời
    results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    
    # Phân tích kết quả
    successful_txs = sum(1 for r in results if r["success"])
    aborted_txs = n - successful_txs
    abort_rate = (aborted_txs / n) * 100
    
    avg_latency = sum(r["latency"] for r in results) / n
    tps = successful_txs / total_time if total_time > 0 else 0
    
    logger.info(f"RESULT (N={n}): Successful: {successful_txs}, Aborted: {aborted_txs}, "
                f"Abort Rate: {abort_rate:.1f}%, Avg Latency: {avg_latency*1000:.1f}ms, TPS: {tps:.2f}")
                
    return {
        "concurrency": n,
        "successful": successful_txs,
        "aborted": aborted_txs,
        "abort_rate": abort_rate,
        "avg_latency_ms": avg_latency * 1000,
        "tps": tps
    }

async def main():
    logger.info("=" * 60)
    logger.info("=== STARTING CONCURRENCY BENCHMARK ===")
    logger.info("=== Đo lường hiệu năng hệ thống phân tán SI dưới áp lực đồng thời cao ===")
    logger.info("=" * 60)
    
    concurrency_levels = [2, 5, 10, 20, 50]
    product_id_to_test = 1  # Sản phẩm sữa trên Store Front
    
    # 1. Chạy đo lường lần lượt cho từng mức độ đồng thời
    benchmark_data = []
    for n in concurrency_levels:
        res = await run_benchmark_for_concurrency(n, product_id_to_test)
        benchmark_data.append(res)
        await asyncio.sleep(1.0) # Nghỉ 1s giữa các đợt test
        
    # 2. Xuất dữ liệu ra file CSV
    logger.info(f"BENCHMARK: Exporting results to {CSV_PATH}...")
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["concurrency", "successful", "aborted", "abort_rate", "avg_latency_ms", "tps"])
        writer.writeheader()
        writer.writerows(benchmark_data)
        
    # 3. Vẽ biểu đồ hiệu năng sử dụng matplotlib
    logger.info("BENCHMARK: Rendering benchmark charts using matplotlib...")
    
    concurrencies = [d["concurrency"] for d in benchmark_data]
    abort_rates = [d["abort_rate"] for d in benchmark_data]
    tps_values = [d["tps"] for d in benchmark_data]
    latencies = [d["avg_latency_ms"] for d in benchmark_data]
    
    # ==========================================
    # Biểu đồ 1: Tỉ lệ Abort (FCW Conflict Rate)
    # ==========================================
    plt.figure(figsize=(8, 5))
    plt.plot(concurrencies, abort_rates, marker='o', color='red', linewidth=2, label="Abort Rate (%)")
    plt.fill_between(concurrencies, abort_rates, alpha=0.15, color='red')
    plt.title("Mối quan hệ giữa Số lượng Client đồng thời và Tỉ lệ Hủy (Abort Rate)\n(Kiểm chứng cơ chế First-Committer-Wins)", fontsize=11)
    plt.xlabel("Số lượng Client đồng thời (N)", fontsize=10)
    plt.ylabel("Tỉ lệ Hủy giao dịch (%)", fontsize=10)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.ylim(-5, 105)
    for x, y in zip(concurrencies, abort_rates):
        plt.annotate(f"{y:.1f}%", (x, y), textcoords="offset points", xytext=(0,10), ha='center', fontsize=9, fontweight='bold', color='darkred')
    plt.tight_layout()
    plt.savefig(ABORT_CHART_PATH, dpi=150)
    plt.close()
    
    # ==========================================
    # Biểu đồ 2: TPS (Throughput) & Latency (kết hợp 2 trục Y)
    # ==========================================
    fig, ax1 = plt.subplots(figsize=(8, 5))
    
    color = 'tab:green'
    ax1.set_xlabel('Số lượng Client đồng thời (N)')
    ax1.set_ylabel('Thông lượng (TPS - Giao dịch/Giây)', color=color)
    line1 = ax1.plot(concurrencies, tps_values, marker='s', color=color, linewidth=2, label="Throughput (TPS)")
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, linestyle="--", alpha=0.4)
    
    ax2 = ax1.twinx()  
    color = 'tab:blue'
    ax2.set_ylabel('Độ trễ trung bình (ms)', color=color)
    line2 = ax2.plot(concurrencies, latencies, marker='^', color=color, linewidth=2, linestyle='--', label="Latency (ms)")
    ax2.tick_params(axis='y', labelcolor=color)
    
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left')
    
    plt.title("Đo lường Thông lượng (TPS) & Độ trễ Trung bình (ms) phân tán\nDưới tác động của xung đột đồng thời cao", fontsize=11)
    fig.tight_layout()
    plt.savefig(TPS_CHART_PATH, dpi=150)
    plt.close()
    
    # ==========================================
    # Biểu đồ 3: Độ trễ Trung bình (Latency Chart riêng biệt)
    # ==========================================
    plt.figure(figsize=(8, 5))
    plt.bar(range(len(concurrencies)), latencies, tick_label=[str(n) for n in concurrencies], 
            color=['#2196F3', '#4CAF50', '#FF9800', '#F44336', '#9C27B0'], alpha=0.85, edgecolor='black', linewidth=0.5)
    plt.plot(range(len(concurrencies)), latencies, marker='D', color='navy', linewidth=2, linestyle='--', label="Trend")
    plt.title("Độ trễ Trung bình Giao dịch (ms) theo Mức độ Đồng thời\n(Transaction Average Latency Benchmark)", fontsize=11)
    plt.xlabel("Số lượng Client đồng thời (N)", fontsize=10)
    plt.ylabel("Độ trễ trung bình (ms)", fontsize=10)
    plt.grid(True, axis='y', linestyle="--", alpha=0.5)
    for i, (x, y) in enumerate(zip(concurrencies, latencies)):
        plt.annotate(f"{y:.1f}ms", (i, y), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9, fontweight='bold')
    plt.legend()
    plt.tight_layout()
    plt.savefig(LATENCY_CHART_PATH, dpi=150)
    plt.close()
    
    logger.info(f"BENCHMARK COMPLETED. Charts saved in {OUTPUT_DIR}/ directory:")
    logger.info(f" - {ABORT_CHART_PATH}")
    logger.info(f" - {TPS_CHART_PATH}")
    logger.info(f" - {LATENCY_CHART_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
