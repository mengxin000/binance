from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
MAIN_PROGRAM = BASE_DIR / "main.py"
RESTART_DELAY_SECONDS = 3.0


def main() -> None:
    print("守护程序已启动；按 Ctrl+C 可停止守护程序和行情程序。")
    while True:
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{started_at}] 启动 main.py")
        try:
            process = subprocess.Popen(
                [sys.executable, str(MAIN_PROGRAM)],
                cwd=str(BASE_DIR),
            )
            return_code = process.wait()
        except KeyboardInterrupt:
            if "process" in locals() and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
            print("\n守护程序已停止。")
            return

        print(
            f"main.py 已退出（退出码 {return_code}），"
            f"{RESTART_DELAY_SECONDS:g} 秒后重新启动。"
        )
        try:
            time.sleep(RESTART_DELAY_SECONDS)
        except KeyboardInterrupt:
            print("\n守护程序已停止。")
            return


if __name__ == "__main__":
    main()
