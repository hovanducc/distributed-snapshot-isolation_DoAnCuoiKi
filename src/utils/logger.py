import logging
import colorama
from colorama import Fore, Style

# Khởi tạo colorama trên Windows
colorama.init(autoreset=True)

class ColorFormatter(logging.Formatter):
    """Bộ định dạng log có màu sắc phục vụ hiển thị trace phân tán trực quan."""
    
    # Định nghĩa màu sắc cho từng loại cấp độ log
    LEVEL_COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, "")
        message = super().format(record)
        
        # Đặc biệt tô màu từ khóa hệ thống phân tán để dễ theo dõi trace
        if "TX_" in message or "TRANSACTION" in message:
            message = f"{Fore.MAGENTA}{Style.BRIGHT}{message}{Style.RESET_ALL}"
        elif "2PC" in message or "PREPARE" in message or "COMMIT" in message or "ABORT" in message:
            message = f"{Fore.BLUE}{Style.BRIGHT}{message}{Style.RESET_ALL}"
        elif "FCW" in message or "CONFLICT" in message or "WRITE SKEW" in message:
            message = f"{Fore.RED}{Style.BRIGHT}{message}{Style.RESET_ALL}"
        elif "SUCCESS" in message or "COMMITTED" in message:
            message = f"{Fore.GREEN}{Style.BRIGHT}{message}{Style.RESET_ALL}"
        elif color:
            message = f"{color}{message}{Style.RESET_ALL}"
            
        return message

def setup_logger(name: str) -> logging.Logger:
    """Thiết lập logger và gắn formatter có màu."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Tránh nhân bản handler nếu hàm được gọi nhiều lần
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        
        formatter = ColorFormatter(
            fmt="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
            datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger
