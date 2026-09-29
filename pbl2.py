#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# pbl2.py - Support module for PBL2
#    2020/2/19
#

import uuid
import hashlib
import sys


MAGNIFICATION_K = 1024
MAGNIFICATION_M = MAGNIFICATION_K * MAGNIFICATION_K
MAGNIFICATION_G = MAGNIFICATION_K * MAGNIFICATION_M
MAGNIFICATION_T = MAGNIFICATION_K * MAGNIFICATION_G
MAGNIFICATION_P = MAGNIFICATION_K * MAGNIFICATION_T
BUFSIZE = 1024


def genkey(token_str):
    token_str_bytes = token_str.encode('utf-8')
    key1 = hashlib.sha256(token_str_bytes)
    key1.update(uuid.uuid4().bytes)
    key2 = hashlib.sha256(token_str_bytes)
    node_str = f"{uuid.getnode():x}"
    key2.update(node_str.encode('utf-8'))
    key_str = f"{token_str_bytes.hex()}:{key1.hexdigest()}:{key2.hexdigest()}"
    return key_str


def repkey(key_str, filename):
    token_str_hex, key1_hex, _ = key_str.split(":")
    local_key2 = hashlib.sha256(bytes.fromhex(token_str_hex))
    local_key2.update(f"{uuid.getnode():x}".encode('utf-8'))
    file_hash = hashlib.sha256(local_key2.digest())
    file_hash.update(bytes.fromhex(key1_hex))
    try:
        f = open(filename, 'rb')
        while True:
            b = f.read(BUFSIZE)
            if b == b'':
                break
            file_hash.update(b)
    except OSError:
        raise
    repkey_str = "{}:{}".format(key_str, file_hash.hexdigest())
    return repkey_str


def keycheck(repkey_str, filename):
    try:
        _, key1_hex, key2_hex, rcv_filehash_str = repkey_str.split(":")
    except ValueError:
        return False
    local_filehash = hashlib.sha256(bytes.fromhex(key2_hex))
    local_filehash.update(bytes.fromhex(key1_hex))
    try:
        f = open(filename, 'rb')
        while True:
            b = f.read(BUFSIZE)
            if b == b'':
                break
            local_filehash.update(b)
    except OSError:
        raise
    if local_filehash.hexdigest() == rcv_filehash_str:
        return True
    return False


def parse_size_str(size_str):
    """Parse file size string with [k, K, m, M]
    to obtain file size in bytes.
    """
    magnification = 1
    mag_char = size_str[-1]
    if mag_char == 'k' or mag_char == 'K':
        magnification = MAGNIFICATION_K
        num_part = size_str[:-1]
    elif mag_char == 'm' or mag_char == 'M':
        magnification = MAGNIFICATION_M
        num_part = size_str[:-1]
    elif mag_char == 'g' or mag_char == 'G':
        magnification = MAGNIFICATION_G
        num_part = size_str[:-1]
    elif mag_char == 't' or mag_char == 'T':
        magnification = MAGNIFICATION_T
        num_part = size_str[:-1]
    elif mag_char == 'p' or mag_char == 'P':
        magnification = MAGNIFICATION_P
        num_part = size_str[:-1]
    else:
        num_part = size_str
    filesize_in_bytes = int(float(num_part) * magnification)
    return filesize_in_bytes


if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.exit(f"Usage: {sys.argv[0]} token_str filename")
    token_str = sys.argv[1]
    filename = sys.argv[2]
    key_str = genkey(token_str)
    print(f"key_str: {key_str}")
    repkey_str = repkey(key_str, filename)
    print(f"repkey_str(key_str, filename): {repkey_str}")
    print(f"keycheck(repkey(key_str, filename), filename): "
          f"{keycheck(repkey_str, filename)}")
