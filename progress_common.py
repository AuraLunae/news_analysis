"""infini_overview.py / infini_fetch.py が共通で使う、進捗表示・ログ・429待機の部品。
同じフォルダに置いておくこと。"""
import datetime
import os
import threading
import time

WAIT_429 = 130   # 秒。HFのRetry-After(120)に余裕を足した値
MAX_TRIES = 12
BEAT = 15        # 秒。長いクエリの実行中に「生きている」表示を出す間隔

_log_path = None


def set_log(path):
    """進捗ログの保存先を決める。別ウィンドウから Get-Content <path> -Wait で追える。"""
    global _log_path
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    _log_path = path


def log(msg):
    line = f"[{datetime.datetime.now():%H:%M:%S}] {msg}"
    print(line, flush=True)
    if _log_path:
        with open(_log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def fmt(sec):
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}"


class Tracker:
    """全体の進み具合（何件中何件）と、残り時間の目安を表示する。
    残り時間は「実際に処理した月」の平均秒数から出す（スキップ分は含めない）。"""

    def __init__(self, total):
        self.total, self.i, self.n, self.t, self.t0 = total, 0, 0, 0.0, time.time()

    def skipped(self, label):
        self.i += 1
        log(f"[{self.i}/{self.total}] {label} 済みのためスキップ")

    def done(self, label, result, sec):
        self.i += 1
        self.n += 1
        self.t += sec
        eta = self.t / self.n * (self.total - self.i)
        log(f"[{self.i}/{self.total}] {label} {result} {sec:.0f}秒"
            f" | 経過 {fmt(time.time() - self.t0)} | 残り約 {fmt(eta)}")


def wait(sec, why):
    log(f"  {why}。{sec}秒待ちます")
    end = time.time() + sec
    while True:
        left = end - time.time()
        if left <= 0:
            return
        time.sleep(min(30, left))
        if left > 30:
            log(f"  待機中… 残り約{int(left - 30)}秒")


def _beat(label, stop):
    t = time.time()
    while not stop.wait(BEAT):
        log(f"    {label} 実行中… {int(time.time() - t)}秒経過")


def run_sql(con, sql, label):
    """SQLを実行する。戻り値: 'ok' / 'missing'（404） / 'ng'。
    429（レート制限）は自動で待って再試行する。"""
    for i in range(MAX_TRIES):
        stop = threading.Event()
        threading.Thread(target=_beat, args=(label, stop), daemon=True).start()
        try:
            con.execute(sql)
            stop.set()
            return "ok"
        except Exception as e:
            stop.set()
            msg = str(e)
            if "429" in msg:
                wait(WAIT_429, f"429（HFのレート制限）。再試行 {i + 1}/{MAX_TRIES}")
            elif "404" in msg:
                return "missing"
            else:
                log(f"  失敗: {msg[:300]}")
                wait(10, "一時的なエラーの可能性があるため再試行")
    return "ng"
