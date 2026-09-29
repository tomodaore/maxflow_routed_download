#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#サーバーの数をまず送ってもらう

#中継サーバー間の帯域測定を命令
#MEASURE_ALL クライアント名　ファイルサーバー名　ファイル名　pgs key 

#経路が決まった時
#クライアント->pg4->pg5->pg6のような経路の時は
#クライアントがpg4に接続して次のような文を送る　pg5 pg6\n


import sys
import socket
import threading
import time 
import subprocess
import re



# --- 設定項目 ---
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

#これがクライアントから送られてくるものとする(クライアントのホスト名は除かれているとする)
#TARGET_ORDER = ["pg1","pg2" , "pg3", "pg4", "pg5",  "pg7"]

BUF_SIZE = 4096
TEST_SIZE_BYTES = 1024*1024
N=1.0 #時間制限

def get_target_relays(my_host,file_server_name,pgs_list):

    #pgsの自分自身より後ろのものをtargetsに入れる
    targets = []
    file_targets=[]

    #配列内の自分の場所を探す
    try:
        my_index = pgs_list.index(my_host)
    except ValueError:
        return [],[]

    #自分自身の位置の一つ後ろから最後までのリストを取得
    
    potential_targets = pgs_list[my_index+1:]

    #リストから実際に測定する相手を選ぶ
    for node in potential_targets:
        
        if node != file_server_name and node in NODE_MAP:
            targets.append(node)

    # --- ファイルサーバーは常に測定対象に追加 ---
    if file_server_name in NODE_MAP:
        file_targets.append(file_server_name)

    return file_targets, targets
    

def recv_line(sock):
    # ソケットから1行受信
    data = b""
    while not data.endswith(b"\n"):
        try:
            chunk = sock.recv(1)
            if not chunk:
                break
            data += chunk
        except:
            break
    return data



def handle_speedtest_generation(client_sock, size_mb):
    
    chunk_data = b'0' * TEST_SIZE_BYTES
    try:
        for _ in range(size_mb):
            client_sock.sendall(chunk_data)
    except:
        pass

#A1提出課題を用いた
def measure_network_quality(target_host):
    
    #pingを実行し (ロス率[0.0-1.0]) を返す
    
    try:
        # -c 5 (5回送信), -W 2 (タイムアウト2秒)
        cmd = ["ping", "-c", "5", "-W", "2", target_host]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if not result.stdout:
            return 1.0

        output = result.stdout

        # 1. パケットロス率を抽出 (例: "0% packet loss")
        loss_match = re.search(r'(\d+)% packet loss', output)
        loss_rate = float(loss_match.group(1)) / 100.0 if loss_match else 1.0
        return loss_rate

    except Exception as e:
        print(f"[!] Ping execution error: {e}")
        return  1.0

# スループット(BPS)を測定する関数
def measure_relay_bps_float(target_host, target_port):
    MAX_TIME = N
    start_all = time.time()

    def do_test(size_mb):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        try:
            sock.connect((target_host, target_port))
            sock.sendall(f"SPEEDTEST_GENERATE {size_mb}\n".encode())

            total = 0
            start_t = time.time()
            expected_bytes = TEST_SIZE_BYTES * size_mb  # ←★ size_mb を使う！

            while total < expected_bytes:
                elapsed = time.time() - start_all
                if elapsed >= MAX_TIME:
                    print(f"[!] Aborted after {elapsed:.1f}s (>30s limit)")
                    break

                remaining = MAX_TIME - elapsed
                sock.settimeout(max(1.0, remaining))

                try:
                    chunk = sock.recv(BUF_SIZE)
                except socket.timeout:
                    print("[!] recv() timed out — stopping measurement")
                    break

                if not chunk:
                    break

                total += len(chunk)

            duration = time.time() - start_t
            bps = (total * 8) / duration if duration > 0 else 0.0
            mbps = bps / 1_000_000
            return mbps, duration

        except Exception as e:
            print(f"[!] BPS test error ({size_mb}MB): {e}")
            return 0.0, 0.0
        finally:
            sock.close()

    mbps, duration = do_test(1)  # ここで1MB分のテスト



    #測定が短い場合
   # if duration > 0 and duration < 12:
    #    print(f"[!] Test finished too fast ({duration:.3f}s), retrying with 5MB...")
     #   mbps, duration = do_test(5)

      #  if duration > 0 and duration < 12:
       #     print(f"[!] Test finished too fast ({duration:.3f}s), retrying with 10MB...")
        #    mbps, duration = do_test(10)

            #if duration > 0 and duration < 15:
            #    print(f"[!] Test finished too fast ({duration:.3f}s), retrying with 25MB...")
            #    mbps, duration = do_test(25)
    
                #if duration > 0 and duration < 15:
                #    print(f"[!] Test finished too fast ({duration:.3f}s), retrying with 50MB...")
                #    mbps, duration = do_test(50)
    
    
    
    return mbps

def measure_file_target_bps(target_host, target_port, filename, key, start_byte, end_byte):
    MAX_TIME = N  # 測定全体の上限時間
    start_all = time.time()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10.0)  # 初期タイムアウトを短く設定
    try:
        s.connect((target_host, target_port))

        cmd = f"GET {filename} {key} PARTIAL {start_byte} {end_byte}\n"
        s.sendall(cmd.encode("utf-8"))
        print(f"[→] {target_host}:{target_port} ← {cmd.strip()}")

        # --- ヘッダ受信 ---
        header = b""
        while not header.endswith(b"\n"):
            elapsed = time.time() - start_all
            if elapsed >= MAX_TIME:
                print("[!] Timeout while waiting for header (30s limit exceeded)")
                return 0.0

            try:
                chunk = s.recv(4096)
            except socket.timeout:
                print("[!] Header recv() timed out")
                return 0.0
            if not chunk:
                raise ConnectionError("Connection closed before header")
            header += chunk

        # --- ヘッダ解析 ---
        header_line, rest = header.split(b"\n", 1)
        header_line = header_line.decode("utf-8", errors="replace").strip()
        if not header_line.startswith("OK "):
            print(f"[!] GET error from {target_host}: {header_line}")
            return 0.0

        match = re.search(r"total\s+(\d+)\s+bytes", header_line)
        if not match:
            print(f"[!] Invalid header: {header_line}")
            return 0.0
        total_bytes = int(match.group(1))

        # --- データ受信 ---
        total_recv = len(rest)
        start_t = time.time()

        while total_recv < total_bytes:
            elapsed = time.time() - start_all
            if elapsed >= MAX_TIME:
                duration = time.time() - start_t
                bps = (total_recv * 8) / duration if duration > 0 else 0.0
                mbps = bps / 1_000_000
                print(f"[!] File GET aborted after {elapsed:.1f}s (>30s limit)")
                return mbps

            remaining = MAX_TIME - elapsed
            s.settimeout(max(1.0, remaining))

            try:
                data = s.recv(BUF_SIZE)
            except socket.timeout:
                print("[!] Data recv() timed out — stopping measurement")
                break

            if not data:
                break
            total_recv += len(data)

        end_t = time.time()
        duration = end_t - start_t
        bps = (total_recv * 8) / duration if duration > 0 else 0.0
        mbps = bps / 1_000_000
        print(f" File GET {filename}: {mbps:.2f} Mbps ({duration:.3f} s)")

        return mbps

    except Exception as e:
        print(f"[!] Error in measure_file_target_bps: {e}")
        return 0.0
    finally:
        s.close()



# --- 並列実行用ワーカー ---
def measurement_worker(target_index, target_host, target_port, results_list, is_file_target=False, filename=None, key=None):
    
    #中継サーバの場合 → SPEEDTEST_GENERATE
    #ファイルサーバの場合 → GET PARTIAL
    
    #loss = measure_network_quality(target_host)

    #if loss == 0.0:
    if is_file_target:
            # ファイルサーバへの帯域測定
            start_byte = 0
            end_byte = 1*1024 * 1024 - 1  # 1MB単位など
            mbps = measure_file_target_bps(target_host, target_port, filename, key, start_byte, end_byte)
    else:
            # 中継サーバとの測定
            mbps = measure_relay_bps_float(target_host, target_port)
    #else:
    #    mbps = 0.0

    results_list[target_index] = mbps

def handle_client(client_sock, client_addr):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        first_line = recv_line(client_sock)
        if not first_line: return

        request_str = first_line.decode("utf-8", errors="replace").strip()
        print(f"[*] Request from {client_addr}: '{request_str}'")
        parts = request_str.split()

        # --- MEASURE_ALL ---
        if len(parts) >= 6 and parts[0] == "MEASURE_ALL":
            my_host = socket.gethostname() 
            client_name = parts[1]
            file_server_name = parts[2]
            file_name = parts[3]
            pgs = parts[4]
            key = parts[5] #中継サーバーがGET_PARTIALを実行するためpbl2.genkey(token_str)の結果を送る
            
            pgs_list=pgs.split(",")
            file_targets,targets = get_target_relays(my_host, file_server_name,pgs_list)
            
            print(f"    -> Measuring targets: {targets} and {file_targets} ")
            
            # bpsを格納するリスト
            bps_array = [0.0]*len(pgs_list)
        
            # スレッドを管理するリスト
            threads = []

            # ターゲットごとにスレッドを作成して一斉スタート
            for target_host in targets:
                target_port = NODE_MAP.get(target_host)
                if not target_port: continue
                idx = pgs_list.index(target_host)
                t = threading.Thread(
                    target=measurement_worker,
                    args=(idx,target_host, target_port, bps_array)
                )
                t.start()
                threads.append(t)

            if file_targets:  # 空でなければ実行
                file_target = file_targets[0]
                target_port = 60623
                idx = pgs_list.index(file_target)
                t = threading.Thread(
                target=measurement_worker,
                args=(idx, file_target, target_port, bps_array,True,file_name,key)
                )
                t.start()
                threads.append(t)
            # 全スレッドが終わるのを待つ
            for t in threads:
                t.join()
             
            response_text = " ".join(map(str, bps_array)) + "\n"
            client_sock.sendall(response_text.encode())
            return

        # --- SPEEDTEST_GENERATE ---

        #SPEEDTEST_GENERATE N\n  の命令で N MBのデータを送信する
        elif len(parts) >= 2 and parts[0] == "SPEEDTEST_GENERATE":
            try:
                size_mb = int(parts[1])
                handle_speedtest_generation(client_sock, size_mb)
            except ValueError:
                pass
            return

        # --- Routing (Normal Relay) ---
        #pg1(クライアント)->pg2->pg3->pg6(ファイルサーバー)の時は
        #1 pg1がpg2と接続して   pg3 pg6\n  を送る
        #2 pg1がpg2にgetコマンドなどを送る
        if len(parts) < 1:
            return

        next_host = parts[0]
        next_port = NODE_MAP.get(next_host)
        remaining_route = parts[1:] 

        if len(remaining_route) == 0:
        # 経路が残っていない＝次が最後のサーバ
        # もし next_host がファイルサーバなら60623
            next_port = 60623
            
        else:
        # まだ経路が残っている（中継サーバ）
            next_port = NODE_MAP.get(next_host)

        try:
            server_sock.connect((next_host, next_port))
        except Exception as e:
            print(f"[!] Connection failed to {next_host}:{next_port} ({e})")
            return
       
        if len(remaining_route) > 0:
            forward_header = " ".join(remaining_route) + "\n"
            server_sock.sendall(forward_header.encode("utf-8"))

        # 中継(Relay)処理における「上り/下り」の同時転送にはスレッドが必要
        # ここでのスレッドは「1つの接続内での送受信」用であり、
        # 「複数のクライアントを同時にさばく」ためのものではないため、ボトルネック測定には影響しない
        def forward_down():
            while True:
                try:
                    data = server_sock.recv(BUF_SIZE)
                    if not data: break
                    client_sock.sendall(data)
                except: break
        
        t = threading.Thread(target=forward_down)
        t.daemon = True
        t.start()

        while True:
            data = client_sock.recv(BUF_SIZE)
            if not data: break
            server_sock.sendall(data)
            
    except: pass
    finally:
        client_sock.close()
        server_sock.close()

def main():
    my_host = socket.gethostname()

    if my_host in NODE_MAP:
        listen_port = NODE_MAP[my_host]
        print(f"[*] Host Auto-Detection: I am '{my_host}'.")
    else:
        print(f"[!] CRITICAL ERROR: Hostname '{my_host}' is not in NODE_MAP.")
        sys.exit(1)

    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        listen_sock.bind(("0.0.0.0", listen_port))
        backlog = max(5, len(NODE_MAP) * 3)
        listen_sock.listen(backlog)
        
        print(f"[*] Relay Server Started on {my_host}:{listen_port}")
    

        while True:
            # 接続待機
            client_sock, client_addr = listen_sock.accept()
            
            # 接続が来たら、handle_client を実行する分身（スレッド）を作って任せる
            # これにより、メインのループはすぐに次の接続待ちに戻れます
            t = threading.Thread(target=handle_client, args=(client_sock, client_addr))
            t.daemon = True
            t.start()
    except KeyboardInterrupt:
        print("\n[*] Stopping Server.")
    except Exception as e:
        print(f"\n[!] Fatal Error: {e}")
    finally:
        listen_sock.close()

if __name__ == "__main__":
    main()
