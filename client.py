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

BUF = 1 * 1024 #fix it bufの数ごとに読み込む。これが大きいほど高速になるがメモリを食う。
test_bytes = 1024*1024  # 1MB　これは帯域測定用のデータ量。これにsizeをかけたものを受信する。
pgs = []
route_ready = False # 経路が計算ずみか
route = []
route1 = []
route2 = []
w1 = 0
w2 = 0

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
    start_byte = 0
    end_byte = BUF - 1
    open(filename,"wb").close() #ここでfilenameのファイルを新規作成しているから追加するときは"ab"で書き込む
    while True:
        
        if route_ready == False:
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
                

                recv_len = end_byte - start_byte + 1
                remaining = recv_len

                with open(filename, "ab") as f:
                    while remaining > 0:
                        chunk = s.recv(min(BUF, remaining))

                        if not chunk:
                            return False
                        f.write(chunk)
                        remaining -= len(chunk)
            except Exception as e:
                log_print(f"GET PARTIAL error {e}")
                time.sleep(1)
                continue

            finally:
                #print(f"GET PARTIAL {start_byte} ~ {end_byte}終了") #デバッグ用
                start_byte += BUF
                if start_byte >= size:
                    break
                end_byte = min(start_byte + BUF - 1, size - 1)
                s.close()
        else:
            ##　ここで分割でダウンロードを実行
            log_print("route_manager ok")
            print("route maneger ok")
            break


def measure_max_bps(pg,server_port,size):
    received = 0
    total_bytes = size*test_bytes
    start = time.time()
        
    with socket(AF_INET,SOCK_STREAM) as s:
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

            with socket(AF_INET,SOCK_STREAM) as s:
                s.connect((pg,server_port))
                msg = f"GET {filename} {key} PARTIAL 0 {total_bytes}\n".encode("utf-8")
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
                end = time.time()
            elapsed = end - start
            mbps = (received * 8) / (elapsed * 1024 * 1024)  # Mbpsに変換
            log_print(f"ファイルサーバからの帯域受信完了: pg={pg}, 受信バイト数={received}, 経過時間={elapsed:.2f}秒, 帯域={mbps:.2f}Mbps")
            return mbps
        except (ConnectionRefusedError, socket.timeout, ConnectionError) as e:
            attempt += 1
            log_print(f"[Warning] ファイルサーバとの通信失敗 (試行 {attempt}/{retries}): {e}")
            time.sleep(1.0)  # 少し待って再試行

def input_pg_bandwidth(pg,server_port,tmp,lock,server_host,file_name,key):
    #ここに中継サーバとクライアント間のスループット計算をするための命令を飛ばして ファイルサーバ間はまだ。
    if pg != server_host:
        #経路計算に使えるようにする。
        size = 1
        mpbs = 0
        while True:
            mbps = measure_max_bps(pg,server_port,size)
            if mbps > size * 0.8:
                size *= 2
            else:
                break
        
        #中継サーバにめいれいを送り、帯域を受信する。
        retries = 3
        attempt = 0
        pg_str = ",".join(pgs)
        while attempt < retries:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((pg, server_port))
                    msg = f"MEASURE_ALL {gethostname()} {server_host} {file_name} {pg_str} {key}\n"

                    s.sendall(msg.encode("utf-8"))
                    log_print(f"中継サーバに帯域要求送信: pg={pg}")

                    buffer = ""
                    while True:
                        data = s.recv(BUF).decode()
                        if not data:
                            raise ConnectionError("接続が途中で切断されました")
                        buffer += data
                        if "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            bps = list(map(float,line.split()))
                            break
                    
                    number_pg = int(pg[2:]) #pgがpg1なら1、pg2なら2を取り出す
                    with lock:
                        tmp[pg] = list(bps)
                        tmp[pg].insert(number_pg -1 ,mbps) #自分自身のところにmbpsを入れる。
                    log_print(f"中継サーバから帯域受信: pg={pg}, bps={bps},tmp[{pg}]={tmp[pg]}")
                    return # 成功したら終了

            except (ConnectionRefusedError, socket.timeout, ConnectionError, json.JSONDecodeError) as e:
                attempt += 1
                log_print(f"[Warning] {pg} との通信失敗 (試行 {attempt}/{retries}): {e}")
                time.sleep(1.0)  # 少し待って再試行

        # 全ての試行が失敗した場合
        with lock:
            tmp[pg].append(1e+30)  # 非常に大きな値を設定
        log_print(f"[Error] {pg} との通信に全て失敗しました。tmp[{pg}] = None と設定")

    else:
        #pgとファイルサーバ名が同じ場合
        arr = [0.0] * (len(pgs) + 1)
        measure_size = 1
        while True:
            bps = measure_file_bps(pg,server_port,file_name,key,measure_size)
            if bps > measure_size * 0.8:
                measure_size *= 2
            else:
                break
            
        pg_num = int(pg[2:])
        arr[pg_num-1] = bps #自分自身のところ
        with lock:
            tmp[pg] = arr
        log_print(f"ファイルサーバとクライアントが同じ: pg={pg}, tmp[{pg}]={tmp[pg]}")
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
    global route
    global route1
    global route2
    global w1
    global w2
    tmp = {pg:[] for pg in pgs}
    thread = []
    lock = threading.Lock()

    for pg in pgs:
        th = threading.Thread(target=input_pg_bandwidth, args=(pg,server_port,tmp,lock,server_host,filename,key,))
        th.start()
        thread.append(th)
    
    for th in thread:
        th.join()
    
    #7x7の帯域行列を作成
    tmp[gethostname()] = [0.0]* (len(pgs)+1)
    pgs_tmp = []
    input_pgs_name(pgs_tmp)
    band = [[0]*len(pgs_tmp) for _ in range(len(pgs_tmp))]
    for i in range(len(pgs_tmp)):
        for j in range(len(tmp)):
            bps = max(tmp[pgs_tmp[i]][j], tmp[pgs_tmp[j]][i])
            band[i][j] = bps
    
    # ここでbandに各中継サーバの帯域が入っている。
    # 経路計算を行う
    start = pgs_tmp.index(gethostname())
    goal = pgs_tmp.index(server_host)

    max_flow, flow, used_paths= edmonds_karp_with_paths(band, start, goal)
    # ★ 追加：flow降順 → hop昇順
    used_paths.sort(key=lambda x: (-x[1], len(x[0])))
    #経路を二つ決めたからなにかしらの形にして保存する。 
    # w1,w2に整数が入ってる。route1,route2には[pg4,pg1,pg6]なら[4,1,6]が入ってる。
    if len(used_paths) != 1: #経路が一個のみの場合
        weight = []
        for i in range(2):#rangeの数は選ぶ経路の数
            path,bw = used_paths[i]
            weight.append(bw)
            route_all = []
            for i in path:
                route_all.append(i+1)
            route.append(route_all)
        for i in range(len(weight)):
            if i == 0:
                w1 = weight[i]
            else:
                w2 = weight[i]
        for i in range(len(route)):
            if i == 0:
                route1 = route[i]
            else:
                route2 = route[i]
    else:
        path,bw = used_paths[0]
        #print("経路:", [f"pg{i+1}" for i in path], "flow:", bw)
        for i in path:
            route1.append(i+1)
        w1 = bw
        #print(f"route1:{route1} w1:{w1}")
        
    #終わったからroute_readyをTrueにする。
    route_ready = True

def threading_download(server_host,server_port,filename,key,size):
    #経路解析までファイルサーバからダウンロードする
    t1 = threading.Thread(target=thread_file_recv, args=(server_host,server_port,filename,key,size,))
    t2 = threading.Thread(target=thread_route_manager, args=(server_host,server_port,filename,key,))

    
    t1.start()#始まり
    t2.start()
    
    t2.join()
    t1.join()#終わるまで


def main():
    open("log.txt","w").close()
    if len(sys.argv) != 5:
        log_print(f"Usage: {sys.argv[0]} server_host server_port filename token_str")
        sys.exit(1)

    server_host = sys.argv[1] #クライアント名
    server_port = int(sys.argv[2]) #ポート番号
    filename = sys.argv[3] #ファイル名
    token_str = sys.argv[4] #トークン文字列
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
