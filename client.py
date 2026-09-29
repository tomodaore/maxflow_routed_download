#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# tmp[pg]にはpgs = ["pg1","pg2","pg3","pg5","pg7"]のときクライアントがpg6の時、
# tmp = {pg1:[0.0,123.4,567.8,90.12,300.0,800.0],etc...}このような感じで入る

import sys
import time
from socket import *
import threading
import pbl2  # pbl2.py が同じディレクトリにある前提
import json
import math
from collections import deque

BUF = 32 * 1024 # bufの数ごとに読み込む。これが大きいほど高速になるがメモリを食う。
buf_recv = 1024 # 受信バッファサイズ
test_bytes = 1024*1024  # 1MB　これは帯域測定用のデータ量。これにsizeをかけたものを受信する。
N_SETTIME = 10.0 #settimeoutの時間
TIMEOUT = 1.0 #timeoutの時間
pgs = []
route_ready = False # 経路が計算ずみか
routes = []     # 例: [[6,4,2], [6,7,5,2], ...]  (pg番号の配列)
weights = []    # 例: [123.4, 56.7, ...]
MAX_ROUTES = 5   # ここを 3 にすれば3経路, 5 にすれば5経路
alpha = 0.9      # 流量の何割を使うか（例: 0.9 → 90%）

CHUNK_SIZE = 32 * 1024

# relay_server 側（pg間中継）の待受ポート
NODE_MAP = {
    "pg1": 54300,
    "pg2": 54300,
    "pg3": 54300,
    "pg4": 54300,
    "pg5": 54300,
    "pg6": 54300,
    "pg7": 54300,
    "pg8": 54300,
    "pg9": 54300,
    "pg10": 54300,
    "pg11": 54300,
    "pg12": 54300,
    "pg13": 54300,
    "pg14": 54300,
}


# ファイルサーバ（file_server=pgX）が待ち受けるポート（班の仕様）
FILE_SERVER_PORT = 60623


def find_bad_tmp(tmp, pgs_tmp):
    N = len(pgs_tmp)
    bad = []
    for pg in pgs_tmp:
        v = tmp.get(pg, None)
        if v is None or len(v) != N:
            bad.append((pg, None if v is None else len(v)))
    return bad



def input_pgs_name(name):
    with open("pgs.txt", "r", encoding="utf-8") as f: #中継サーバリスト読み込み
        for line in f:
            a = line.strip()   # 改行を除去
            if a:
                name.append(a)
    
def log_print(msg):#log.txtに出力する。
    with open("log.txt","a") as f:
        print(msg,file=f)

def recv_line(sock):
    """ソケットから1行（改行まで）を受信して文字列で返す"""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode("utf-8", errors="replace")


def do_size(server_host, server_port, filename):
    """SIZE コマンドを送り、サイズ（バイト数）を返す。失敗時は None。"""
    s = socket(AF_INET, SOCK_STREAM) #ソケット作成
    try:
        s.connect((server_host, server_port)) #ソケット接続
        log_print(f"ソケット接続: server_host={server_host},server_port={server_port}")
        msg = f"SIZE {filename}\n".encode("utf-8")
        s.sendall(msg)

        line = recv_line(s).strip()
        ##recv_line(s)で改行 \n が来るまでサーバからデータを読み込む
        #.strip() で先頭末尾の空白や \n を取り除く
        # 例: OK somefile.txt 123456 bytes
        #     NG 101 No such file
        if not line.startswith("OK "):
            log_print(f"SIZE error {line}")
            return None

        parts = line.split()
        # parts = ["OK", "somefile.txt", "123456", "bytes"]
        size = int(parts[2])
        return size
    finally:
        log_print(f"SIZE 終了")
        s.close()


def do_get_all(server_host, server_port, filename, key, expected_size):
    """
    GET filename key ALL を送り、ファイル全体を受信して filename に保存する。
    expected_size は SIZE で得たサイズ（検査用）。
    成功すれば True, 失敗なら False。
    """
    s = socket(AF_INET, SOCK_STREAM)
    try:
        s.connect((server_host, server_port))
        log_print(f"ソケット接続: server_host={server_host},server_port={server_port}")
        msg = f"GET {filename} {key} ALL\n".encode("utf-8")
        s.sendall(msg)
        log_print(f"GET送信: filename={filename}")

        # 1行目のヘッダを読む
        header = recv_line(s).strip()
        # 例: OK Sending some_file.txt from 0 to 1023 total 1024 bytes at ...
        if not header.startswith("OK "):
            log_print(f"GET error {header}")
            return False,None

        parts = header.split()
        # ["OK", "Sending", "some_file.txt", "from", "0", "to", "1023",
        #  "total", "1024", "bytes", "at", "...."]
        try:
            idx_total = parts.index("total")
            total_bytes = int(parts[idx_total + 1])
        except (ValueError, IndexError) as e:
            log_print(f"GET error {e}")
            return False,None

        # expected_size と total_bytes が合っているか一応チェック パケットロス対策
        if expected_size is not None and total_bytes != expected_size:
            log_print(f"Warning: SIZE と GET の total が一致しません: expect_size={expected_size},total_size={total_bytes}")
            
        # データ本体（total_bytes バイト）を受信してファイルに保存
        remaining = total_bytes
        recv_bytes = 0
        start_time = time.time()

        with open(filename, "wb") as f:
            while remaining > 0:
                chunk = s.recv(min(BUF, remaining))
                if not chunk:
                    log_print("Connection closed before receiving all data")
                    return False,None
                f.write(chunk)
                remaining -= len(chunk)
                n = len(chunk)
                recv_bytes += n

        end_time = time.time()
        recv_time = end_time - start_time

        bps = do_get_bps(recv_bytes, recv_time)

        return True, bps
    finally:
        log_print(f"GET ALL 終了")
        s.close()

def do_get_bps(recv_bytes, recv_time):
    #bpsを計算、recv_time が0以下のときはNoneを返す
    if recv_time <= 0:
        print("Error: bps = 0")
        return None

    bps = 8 * recv_bytes / recv_time
    print(f"bps: {bps:.3f} bit/s")
    return bps


def do_rep(server_host, server_port, filename, repkey_str):
    """REP filename repkey_str を送り、サーバからの応答を表示する。"""
    s = socket(AF_INET, SOCK_STREAM)
    try:
        s.connect((server_host, server_port))
        log_print(f"ソケット接続: server_host={server_host},server_port={server_port}")
        msg = f"REP {filename} {repkey_str}\n".encode("utf-8")
        s.sendall(msg)
        log_print(f"REP送信: filename={filename}")

        # サーバからの応答を1行読む（必須ではないがデバッグ用途）
        line = recv_line(s).strip()
        log_print(f"REP reply {line}")
        print("REP reply:", line)

    finally:
        log_print(f"REP 終了")
        s.close()


def thread_file_recv(server_host,server_port,filename,key,size):
    print(f"t1 start")
    start_byte = 0
    end_byte = buf_recv - 1
    open(filename,"wb").close() #ここでfilenameのファイルを新規作成しているから追加するときは"ab"で書き込む
    while True:

        if route_ready == False:
            s = None
            try:
                s = socket(AF_INET,SOCK_STREAM)
                s.connect((server_host,server_port))
                msg = f"GET {filename} {key} PARTIAL {start_byte} {end_byte}\n".encode("utf-8")#partial送信
                s.sendall(msg)

                header = recv_line(s).strip()
                if not header.startswith("OK "):
                    print(f"GET PARTIAL error {header}")
                    return False

                parts = header.split()
                # ["OK", "Sending", "some_file.txt", "from", "0", "to", "1023",
                #  "total", "1024", "bytes", "at", "...."]
                try:
                    idx_total = parts.index("total")
                    total_bytes = int (parts[idx_total + 1])
                except (ValueError,IndexError) as e:
                    print(f"GET error {e}")
                    return False

                # partial は「要求した範囲だけ」受信すればいいので、recv_len を使う
                recv_len = end_byte - start_byte + 1
                remaining = recv_len

                with open(filename, "ab") as f:
                    while remaining > 0:
                        chunk = s.recv(min(buf_recv, remaining))
                        if not chunk:
                            return False
                        f.write(chunk)
                        remaining -= len(chunk)

                #実際に受信した範囲で進める
                start_byte = end_byte + 1

                if start_byte >= size:
                    break
                end_byte = min(start_byte + buf_recv - 1, size - 1) #fix it
                print(f"end_byte:{end_byte}")

            except Exception as e:
                log_print(f"GET PARTIAL error {e}")
                time.sleep(1)
                continue

            finally:
                #print(f"GET PARTIAL {start_byte} ~ {end_byte}終了") #デバッグ用
                if s is not None:
                    s.close()

        else:
            ##　ここで分割でダウンロードを実行
            local_routes = [r[:] for r in routes]      # シャローコピーで十分
            local_weights = [float(w) for w in weights]

            log_print("route_manager ok")
            print("route maneger ok")
            log_print(f"ここから分割ダウンロード:{start_byte}")

            if not local_routes:
                log_print("ERROR: routes is empty")
                return False

            # 全routeの終点（ファイルサーバ）が一致していることを確認
            fs0 = route_nums_to_file_server(local_routes[0])
            for rr in local_routes[1:]:
                if route_nums_to_file_server(rr) != fs0:
                    print(f"ERROR: routes の file server が一致しません: {fs0} vs {route_nums_to_file_server(rr)}")
                    return False

            fs_host, fs_port = fs0

            print("Transmitting file...")

            # チャンク分割
            ranges = make_ranges(size, start_byte, CHUNK_SIZE)
            num_chunks = len(ranges)

            # weights比で配分（ログ用）
            counts = calc_chunk_counts(num_chunks, local_weights)
            print(f"chunks={num_chunks}, counts={counts}, weights={local_weights}")

            ok = do_get_parallel_grouped_n(filename, key, start_byte, ranges, size, local_routes, local_weights)
            if not ok:
                print("GET failed")
                return False
            break


    return True


def measure_max_bps(pg,server_port,size):
    received = 0
    total_bytes = size*test_bytes
    start = time.time()
        
    with socket(AF_INET,SOCK_STREAM) as s:
        s.settimeout(N_SETTIME)
        s.connect((pg,server_port))
        msg = f"SPEEDTEST_GENERATE {size}\n"
        s.sendall(msg.encode("utf-8"))
        log_print(f"中継サーバに帯域生成命令送信: pg={pg}, size={size}")
        
        while received < total_bytes:
            data = s.recv(min(BUF,total_bytes - received))
            if not data:
                log_print(f"[Error] 中継サーバからのデータ受信中に接続が切れました: pg={pg}")
                break
            received += len(data)
            #log_print(f"[max_bps] recv={received}/{total_bytes} pg={pg}") print
            if time.time() - start > TIMEOUT:
                end = time.time()
                elapsed = end - start
                mbps = (received * 8) / (elapsed * 1024 * 1024)  # Mbpsに変換
                log_print(f"中継サーバからの帯域受信完了: pg={pg}, 受信バイト数={received}, 経過時間={elapsed:.2f}秒, 帯域={mbps:.2f}Mbps")
                return mbps 
            
    end = time.time()
    elapsed = end - start
    mbps = (received * 8) / (elapsed * 1024 * 1024)  # Mbpsに変換
    log_print(f"中継サーバからの帯域受信完了: pg={pg}, 受信バイト数={received}, 経過時間={elapsed:.2f}秒, 帯域={mbps:.2f}Mbps")
    return mbps
    
    
def measure_file_bps(pg,server_port,filename,key,size):
    received = 0
    retries = 3
    attempt = 0
    total_bytes = size*test_bytes
    while attempt < retries:
        try:
            received = 0
            with socket(AF_INET,SOCK_STREAM) as s:
                s.settimeout(N_SETTIME)
                s.connect((pg,server_port))
                msg = f"GET {filename} {key} PARTIAL 0 {total_bytes - 1}\n".encode("utf-8")
                s.sendall(msg)
                start = time.time()
                
                header = recv_line(s).strip()
                if not header.startswith("OK "):
                    log_print(f"GET PARTIAL error {header}")
                    return -1.0
                parts = header.split()
                try:
                    idx_total = parts.index("total")
                    total_bytes = int (parts[idx_total + 1])
                except (ValueError,IndexError) as e:
                    log_print(f"GET error {e}")
                    return -1.0
                remaining = total_bytes
                while received < remaining:
                    data = s.recv(min(BUF,remaining - received))
                    if not data:
                        log_print(f"[Error] ファイルサーバからのデータ受信中に接続が切れました: pg={pg}")
                        break
                    received += len(data)
                    #log_print(f"[file_bps] recv={received}/{remaining}") print
                    if time.time() - start > TIMEOUT:
                        end = time.time()
                        elapsed = end - start
                        mbps = (received * 8) / (elapsed * 1024 * 1024)  # Mbpsに変換
                        log_print(f"ファイルサーバからの帯域受信完了: pg={pg}, 受信バイト数={received}, 経過時間={elapsed:.2f}秒, 帯域={mbps:.2f}Mbps")
                        return mbps
                    
                end = time.time()
            elapsed = end - start
            mbps = (received * 8) / (elapsed * 1024 * 1024)  # Mbpsに変換
            log_print(f"ファイルサーバからの帯域受信完了: pg={pg}, 受信バイト数={received}, 経過時間={elapsed:.2f}秒, 帯域={mbps:.2f}Mbps")
            return mbps
        except (ConnectionRefusedError, timeout, ConnectionError) as e:
            attempt += 1
            log_print(f"[Warning] ファイルサーバとの通信失敗 (試行 {attempt}/{retries}): {e}")
            time.sleep(1.0)  # 少し待って再試行

    log_print(f"[measure_file_bps][{pg}] return=0.0 reason=retries_exhausted retries={retries}")
    return 0.0

def input_pg_bandwidth(pg, server_port, tmp, lock, server_host, file_name, key):
    # ここに中継サーバとクライアント間のスループット計算をするための命令を飛ばして
    # ファイルサーバ間はまだ。

    pgs_tmp = []
    input_pgs_name(pgs_tmp)                 #中継サーバリスト読み込み（pgs.txt）
    n = len(pgs_tmp)                        #n を pgs.txt の長さに統一（例: 7）
    pg_str = ",".join(pgs)              #relayへ送る列順も pgs.txt 順に固定

    # ★ ADD:
    # client(gethostname()) の列 index を求める（例: client=pg6 なら client_idx=5）
    me = gethostname()
    if me in pgs_tmp:
        client_idx = pgs_tmp.index(me)
    else:
        client_idx = None  # 念のため（本番では起きない想定）

    if pg != server_host:
        # 経路計算に使えるようにする。
        # ループは廃止し、固定サイズで1回だけ測る
        size = 1  # size * test_bytes が 1MB になる想定
        mbps = 1
        start_time = time.time()
        try:
            mbps = measure_max_bps(pg,server_port,size)
        except Exception as e:
            mbps = 0.0
            log_print(f"[Warning] measure_max_bps failed: pg={pg} err={e}")
        end_time = time.time()
        print(f"measure_max_bps finished:pg={pg},time={end_time - start_time}s")
        
        # 中継サーバに命令を送り、帯域を受信する。
        retries = 3
        start_time = time.time()
        for attempt in range(1, retries + 1):
            try:
                with socket(AF_INET, SOCK_STREAM) as s:
                    
                    s.connect((pg, server_port))

                    # pgs は pgs.txt 順の全ノード（例: pg1..pg7）を送る
                    msg = f"MEASURE_ALL {gethostname()} {server_host} {file_name} {pg_str} {key}\n"
                    s.sendall(msg.encode("utf-8"))
                    log_print(f"中継サーバに帯域要求送信: pg={pg}")

                    buffer = ""
                    while True:
                        data = s.recv(BUF).decode("utf-8", errors="replace")
                        if not data:
                            raise ConnectionError("接続が途中で切断されました")
                        buffer += data
                        if "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            bps = list(map(float, line.split()))
                            break

                # bps の長さを n (=pgs.txtの長さ) にそろえる
                if len(bps) < n:
                    bps = bps + [0.0] * (n - len(bps))
                elif len(bps) > n:
                    bps = bps[:n]

                with lock:
                    tmp[pg] = list(bps)

                    # ★ CHANGE:
                    # client列（例: pg6列）に client<->pg の mbps を入れる
                    if client_idx is not None:
                        tmp[pg][client_idx] = mbps
                        log_print(f"[OK] tmp[{pg}] len={len(tmp[pg])} set_client idx={client_idx}")
                    else:
                        log_print(f"[Warning] client_idx is None (me={me})")

                end_time = time.time()
                log_print(f"中継サーバから帯域受信: pg={pg}, tmp[{pg}]={tmp[pg]},time={end_time - start_time}")
                print(f"input_pg_bandwidth finished: pg={pg},time={end_time - start_time}s")
                return  # 成功したら終了

            except (ConnectionRefusedError, timeout, ConnectionError, ValueError) as e:
                log_print(f"[Warning] {pg} との通信失敗 (試行 {attempt}/{retries}): {e}")
                time.sleep(1.0)

        # 全ての試行が失敗した場合
        with lock:
            # routing で選ばれにくくする（帯域0扱い）
            tmp[pg] = [0.0] * n
            # ★ ADD: 通信失敗でも client列だけは measure_max_bps の結果を残す（取れていれば）
            if client_idx is not None:
                tmp[pg][client_idx] = mbps

        log_print(f"[Error] {pg} との通信に全て失敗しました。tmp[{pg}] を 0.0 で埋める")
        return

    else:
        # pgとファイルサーバ名が同じ場合
        arr = [0.0] * n

        # ファイルサーバとクライアント間の帯域を測定
        # ※ 倍々ループは廃止し、固定サイズで1回だけ測る
        measure_size = 1
        bps = 1
        cnt = 0 #カウント用
        tmp_bps1 = 1
        start_time = time.time()
        try:
            bps = measure_file_bps(pg, server_port, file_name, key, measure_size)
        except Exception as e:
            bps = 0.0
            log_print(f"[Warning] measure_file_bps failed: pg={pg} err={e}")

        # ★ CHANGE:
        # file server 行でも client列（例: pg6列）に bps を入れる
        if client_idx is not None:
            arr[client_idx] = bps
        else:
            # 念のため（本番では起きない想定）
            pg_num = int(pg[2:])
            arr[pg_num - 1] = bps

        with lock:
            tmp[pg] = arr
        log_print(f"ファイルサーバとクライアントが同じ: pg={pg}, tmp[{pg}]={tmp[pg]}")
        end_time = time.time()
        print(f"measure_file_bps finished: pg={pg},time={end_time - start_time}")
        return


def edmonds_karp_with_paths(capacity, source, sink):
    n = len(capacity)
    flow = [[0]*n for _ in range(n)]
    max_flow = 0
    used_paths = []  # ★ 追加：実際に流れた経路

    while True:
        parent = [-1]*n
        parent[source] = source
        q = deque([source])

        # BFS
        while q and parent[sink] == -1:
            u = q.popleft()
            for v in range(n):
                if parent[v] == -1 and capacity[u][v] - flow[u][v] > 0:
                    parent[v] = u
                    q.append(v)

        if parent[sink] == -1:
            break

        # ボトルネック
        increment = math.inf
        v = sink
        while v != source:
            u = parent[v]
            increment = min(increment, capacity[u][v] - flow[u][v])
            v = u

        # ★ 経路を復元
        path = []
        v = sink
        while v != source:
            path.append(v)
            v = parent[v]
        path.append(source)
        path.reverse()

        # フロー更新
        v = sink
        while v != source:
            u = parent[v]
            flow[u][v] += increment
            flow[v][u] -= increment
            v = u

        max_flow += increment
        used_paths.append((path, increment))  # ★ 保存

    return max_flow, flow, used_paths

def thread_route_manager(server_host,server_port,filename,key): #中継サーバに経路計算依頼の命令を飛ばす、帰ってきた帯域をもとに経路を決定する。
    # 中継サーバから帯域を受信する　マルチスレッドを用いる
    global route_ready
    global routes
    global weights

    time_start = time.time() #計測
    pgs_tmp = []
    input_pgs_name(pgs_tmp)                 #中継サーバリスト読み込み（pgs.txt）
    N = len(pgs_tmp)

    tmp = {pg:[] for pg in pgs}
    thread = []
    lock = threading.Lock()

    # tmp を最初から長さ N で固定しておく（lenズレ根絶）
    # pgs_tmp にある全ノード分を確実に作る
    for name in pgs_tmp:
        tmp[name] = [0.0] * N

    for pg in pgs:
        port = FILE_SERVER_PORT if pg == server_host else NODE_MAP[pg]
        th = threading.Thread(target=input_pg_bandwidth, args=(pg,port,tmp,lock,server_host,filename,key))
        th.start()
        thread.append(th)

    for th in thread:
        th.join()

    # ★ CHANGE:
    # ここは不要（上で tmp[name]=[0.0]*N を pgs_tmp 全部に対して作っている）
    # tmp[gethostname()] = [0.0]* (len(pgs)+1)

    # ★★★ ここ！！！（band を作る直前） ★★★
    #log_print(f"tmp:{tmp}") tmp内確認
    bad = find_bad_tmp(tmp, pgs_tmp)
    if bad:
        log_print(f"[ERROR] tmp length mismatch: expected={len(pgs_tmp)} bad={bad}")
        # 原因調査中なら、ここで止めるのが安全
        # return

    band = [[0]*len(pgs_tmp) for _ in range(len(pgs_tmp))]
    for i in range(len(pgs_tmp)):
        for j in range(len(pgs_tmp)):
            bps = max(tmp[pgs_tmp[i]][j], tmp[pgs_tmp[j]][i])
            band[i][j] = bps
    
    log_print(f"band\n")
    for i in range(len(pgs_tmp)):
        log_print(f"{band[i]}")

    # ここでbandに各中継サーバの帯域が入っている。
    # 経路計算を行う
    start = pgs_tmp.index(gethostname())
    goal = pgs_tmp.index(server_host)

    max_flow, flow, used_paths= edmonds_karp_with_paths(band, start, goal)
    # ★ 追加：flow降順 → hop昇順
    used_paths.sort(key=lambda x: (-x[1], len(x[0])))

    # cnt = 0
    log_print(f"used_paths:{used_paths}")
    # for path, f in used_paths:
    #     if cnt >= MAX_ROUTES:
    #         break
    #     # path はノード番号のリストなので pg番号リストに変換
    #     route = [int(pgs_tmp[i][2:]) for i in path]
    #     routes.append(route)
    #     weights.append(f)
    #     cnt += 1
    #     log_print(f"routes:{routes}, weights:{weights}")
    
    total_flow = sum(f for _, f in used_paths)
    acc = 0
    for path,f in used_paths:
        route = [int(pgs_tmp[i][2:]) for i in path]
        routes.append(route)
        weights.append(f)
        acc += f
        log_print(f"routes:{routes}, weights:{weights}")
        if acc >= total_flow *alpha:
            break

    #終わったからroute_readyをTrueにする。
    route_ready = True

    time_end = time.time()
    log_print(f"time:{time_end - time_start}秒")


def threading_download(server_host,server_port,filename,key,size):
    #経路解析までファイルサーバからダウンロードする
    t1 = threading.Thread(target=thread_file_recv, args=(server_host,server_port,filename,key,size,))
    t2 = threading.Thread(target=thread_route_manager, args=(server_host,server_port,filename,key,))

    
    t1.start()#始まり
    t2.start()
    
    t2.join()
    log_print(f"t2 finished")
    t1.join()#終わるまで
    log_print(f"t1 finished")

def pg(n: int) -> str:
    """数字 7 → 'pg7' のように変換"""
    return f"pg{n}"

def make_ranges(total_size: int, start_byte: int, chunk_size: int = CHUNK_SIZE):
    """start_byte..total_size-1 を chunk_size ごとに分割して (i,start,end) を作る"""
    if total_size <= 0 or start_byte >= total_size:
        return []
    ranges = []
    i = 0
    start = start_byte
    while start < total_size:
        end = min(start + chunk_size - 1, total_size - 1)
        ranges.append((i, start, end))
        i += 1
        start += chunk_size
    return ranges


def correct_chunks(filename, ranges, result_dict, start_byte):
    """受け取ったチャンクを順に結合してfilename に追加で書き込む"""
    with open(filename, "r+b") as f:
        f.seek(start_byte)
        total_recv = 0
        for i, _, _ in ranges:
            data = result_dict[i]
            f.write(data)
            total_recv += len(data)
    return total_recv


#route_numsの最後をファイルサーバとして扱うための関数
def route_nums_to_file_server(route_nums):
        #例: [1,7,4,5] の最後 5 → host=pg5, port=FILE_SERVER_PORT
    fs_host = pg(route_nums[-1])
    return fs_host, FILE_SERVER_PORT


#クライアントが最初の中継サーバに送る経路ヘッダを作る関数
#中継専用
def build_route_header_full(route_nums) -> str:
    #route_numsの最後をファイルサーバの接続先に変更
    fs_host, fs_port = route_nums_to_file_server(route_nums)

    #中継が１つの時
    if len(route_nums) == 3:
        return f"{fs_host}\n"
    
    #中継が２つ以上の時
    hops = []
    for n in route_nums[2:-1]:
        h = pg(n)
        if h not in NODE_MAP:
            raise KeyError(f"NODE_MAP に {h} がありません")
        hops.append(h)

    # 最後はファイルサーバ（ホスト名のみ）
    hops.append(fs_host)
    return " ".join(hops) + "\n"

def calc_chunk_counts(num_chunks: int, weights_list) -> list:
    """
    num_chunks 個のチャンクを weights 比で各経路へ割り当てる。
    戻り値 counts は len(weights_list) 要素で sum(counts)==num_chunks 。
    端数は最大剰余法（largest remainder method）で配分する。
    """
    if num_chunks <= 0:
        return [0] * len(weights_list)

    if not weights_list:
        return []

    ws = [float(w) if w is not None else 0.0 for w in weights_list]
    ws = [w if w > 0 else 0.0 for w in ws]
    m = len(ws)

    total = sum(ws)
    if total <= 0.0:
        counts = [0] * m
        counts[0] = num_chunks
        return counts

    raw = [num_chunks * (w / total) for w in ws]
    base = [int(math.floor(x)) for x in raw]
    used = sum(base)
    rem = num_chunks - used

    frac = [(raw[i] - base[i], i) for i in range(m)]
    frac.sort(reverse=True)

    counts = base[:]
    for k in range(rem):
        _, idx = frac[k % m]
        counts[idx] += 1

    return counts


#各経路でダウンロードを行うための関数
def worker(route_nums, jobs, tag: str,
           filename: str, key: str,
           result_dict: dict, lock: threading.Lock, ok_flag: dict) -> None:

    #route_nums は完全形:[client, relay1, ..., relayLast, fileServer]
    #接続先は再保の中継サーバであるroute_nums[1]につなぐ
    #connect直後に「次ホップ列（ヘッダ）」を1行送る
    #その後、担当チャンクのGET PARTIALを順番に投げる
    i = -1
    try:
        if len(route_nums) < 2:
            raise ValueError(f"[{tag}] invalid route: {route_nums}")

        # 担当チャンクを順にGET PARTIAL
        for (i, start, end) in jobs:

            s = None
            try:
                if len(route_nums) == 2:
                    #直結だった場合
                    fs_host, fs_port = route_nums_to_file_server(route_nums)
                    s = socket(AF_INET, SOCK_STREAM)
                    s.settimeout(15.0)
                    s.connect((fs_host, fs_port))
                    log_print(f"[{tag}] direct connect {fs_host}:{fs_port} (chunk#{i})")
                else:
                    #最初の中継サーバに接続
                    first_relay = pg(route_nums[1])
                    if first_relay not in NODE_MAP:
                        raise KeyError(f"[{tag}] NODE_MAP に {first_relay} がありません")
                    first_port = NODE_MAP[first_relay]
                    s = socket(AF_INET, SOCK_STREAM)
                    s.settimeout(15.0)
                    ip = gethostbyname(first_relay)
                    log_print(f"[{tag}] connect to {first_relay} ip={ip} port={first_port} route={route_nums} (chunk#{i})")
                    s.connect((ip, first_port))
                    log_print(f"[{tag}] connect {first_relay}:{first_port} route={route_nums} (chunk#{i})")

                    # connect直後に経路ヘッダを送る
                    header_line = build_route_header_full(route_nums)
                    if not header_line.endswith("\n"):
                        header_line += "\n"
                    s.sendall(header_line.encode("utf-8"))

                # GET を送信
                s.sendall(f"GET {filename} {key} PARTIAL {start} {end}\n".encode("utf-8"))

                #帰ってくるヘッダの検査
                header = recv_line(s).strip()
                if not header.startswith("OK "):
                    log_print(f"[{tag} chunk {i}] GET error: {header}")
                    with lock:
                        ok_flag["ok"] = False
                        result_dict[i] = None
                    return

                #何バイト受信すればいいかの確認
                parts = header.split()
                try:
                    idx_total = parts.index("total")
                    total_bytes = int(parts[idx_total + 1])
                except (ValueError, IndexError) as e:
                    log_print(f"[{tag} chunk {i}] header parse error: {e} header={header}")
                    with lock:
                        ok_flag["ok"] = False
                        result_dict[i] = None
                    return

                # total_bytes を受信
                remaining = total_bytes
                buf = bytearray()
                while remaining > 0:
                    data = s.recv(min(BUF, remaining))
                    if not data:
                        log_print(f"[{tag} chunk {i}] connection closed early")
                        with lock:
                            ok_flag["ok"] = False
                            result_dict[i] = None
                        return
                    buf.extend(data)
                    remaining -= len(data)

                #受信したチャンクを共有辞書に格納
                with lock:
                    result_dict[i] = bytes(buf)

            #例外処理（チャンク単位）
            except Exception as e:
                log_print(f"[{tag} chunk {i}] Exception: {e}")
                with lock:
                    ok_flag["ok"] = False
                return

            #チャンクごとにソケットを閉じる
            finally:
                if s is not None:
                    s.close()

    #例外処理（worker全体）
    except Exception as e:
        log_print(f"[{tag}] Exception: {e}")
        with lock:
            ok_flag["ok"] = False
    finally:
        log_print(f"[{tag}] end")


def do_get_parallel_grouped_n(filename: str, key: str, start_byte: int, ranges, expected_size: int,
                             routes_list: list, weights_list: list) -> bool:
    """
    n経路版：routes_list の各経路に weights_list 比でチャンクを割当て並列GETして結合する。
    """
    n_chunks = len(ranges)
    if n_chunks == 0:
        return True

    if not routes_list or not weights_list or len(routes_list) != len(weights_list):
        log_print("[ERROR] do_get_parallel_grouped_n: routes/weights mismatch")
        return False

    # 経路が空のものや weight<=0 を除外（安全のため）
    filtered_routes = []
    filtered_weights = []
    for r, w in zip(routes_list, weights_list):
        if r and (w is not None) and (float(w) > 0.0):
            filtered_routes.append(r)
            filtered_weights.append(float(w))

    if not filtered_routes:
        log_print("[ERROR] do_get_parallel_grouped_n: no valid routes after filtering")
        return False

    routes_list = filtered_routes
    weights_list = filtered_weights

    counts = calc_chunk_counts(n_chunks, weights_list)

    # 連続区間で jobs を作る（従来の前半/後半思想を一般化）
    jobs_list = [[] for _ in range(len(routes_list))]
    pos = 0
    for ridx, c in enumerate(counts):
        if c <= 0:
            continue
        jobs_list[ridx] = ranges[pos:pos + c]
        pos += c
    if pos < n_chunks:
        # 念のため、余りは最後へ
        jobs_list[-1].extend(ranges[pos:])

    result_dict = {}
    lock = threading.Lock()
    ok_flag = {"ok": True}
    threads = []

    for idx, jobs in enumerate(jobs_list):
        if not jobs:
            continue
        tag = f"route{idx+1}"
        t = threading.Thread(
            target=worker,
            args=(routes_list[idx], jobs, tag,
                  filename, key,
                  result_dict, lock, ok_flag)
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if not ok_flag["ok"]:
        return False

    for i, _, _ in ranges:
        if result_dict.get(i) is None:
            return False

    total_recv = correct_chunks(filename, ranges, result_dict, start_byte)
    log_print(f"correct_chunks done: total_recv={total_recv} expected_size={expected_size}")

    remaining_size = expected_size - start_byte
    return total_recv == remaining_size


def main():
    open("log.txt","w").close()
    if len(sys.argv) != 4:
        log_print(f"Usage: {sys.argv[0]} server_host server_port filename token_str")
        sys.exit(1)

    server_host = sys.argv[1] #クライアント名
    server_port = FILE_SERVER_PORT
    filename = sys.argv[2] #ファイル名
    token_str = sys.argv[3] #トークン文字列
    log_print(f"Parameters: server_host={server_host}, server_port={server_port}, filename={filename}, token_str={token_str}")
    input_pgs_name(pgs)
    pgs.remove(gethostname())#今いるクライアントサーバ名削除

    
    # 競技時間計測開始
    t_start = time.time()

    # 1. SIZE でファイルサイズ確認
    size = do_size(server_host, server_port, filename)
    if size is None:
        sys.exit(1)
        
    print("Transmitting file...")

    # 2. クライアントで genkey（ここが「クライアントのみで実行」のポイント）
    key = pbl2.genkey(token_str)
    
    threading_download(server_host,server_port,filename,key,size)

    # 4. ファイルが揃ったので repkey を計算
    repkey_str = pbl2.repkey(key, filename)

    # 5. REP filename repkey_str を送信
    do_rep(server_host, server_port, filename, repkey_str)

    # 競技時間計測終了
    t_end = time.time()
    elapsed = t_end - t_start

    print(f"File transfer finished: Transmission time: {elapsed:.3f} sec")


if __name__ == "__main__":
    main()
