import os
import signal
import subprocess
import sys
import time


PORT = str(os.getenv("PORT", "7777"))
RUN_VK_BOT = os.getenv("RUN_VK_BOT", "0").strip().lower() in {"1", "true", "yes", "on"}


def _terminate_processes(processes: list[subprocess.Popen]) -> None:
    for proc in processes:
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass

    deadline = time.time() + 10
    while time.time() < deadline:
        alive = [proc for proc in processes if proc.poll() is None]
        if not alive:
            return
        time.sleep(0.2)

    for proc in processes:
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass


def main() -> int:
    processes: list[subprocess.Popen] = []
    web_proc: subprocess.Popen | None = None

    def _handle_signal(signum, _frame):
        print(f"[launcher] Получен сигнал {signum}, останавливаю процессы...")
        _terminate_processes(processes)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if RUN_VK_BOT:
        bot_proc = subprocess.Popen([sys.executable, "main.py"])
        processes.append(bot_proc)
        print(f"[launcher] VK bot started with PID {bot_proc.pid}")
    else:
        print("[launcher] RUN_VK_BOT is disabled, starting web server only")

    web_proc = subprocess.Popen(
        ["gunicorn", "-b", f"0.0.0.0:{PORT}", "app:app"]
    )
    processes.append(web_proc)
    print(f"[launcher] Web server started on 0.0.0.0:{PORT} with PID {web_proc.pid}")

    while True:
        for proc in list(processes):
            proc_exit = proc.poll()
            if proc_exit is not None:
                print(f"[launcher] Process PID {proc.pid} exited with code {proc_exit}")
                processes.remove(proc)

                if proc is web_proc:
                    _terminate_processes(processes)
                    return proc_exit
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
