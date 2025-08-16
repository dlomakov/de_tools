#!/usr/bin/env python3
import subprocess
import sys

def human(n: int) -> str:
    units = ["B","KiB","MiB","GiB","TiB","PiB"]
    n = float(n); u = 0
    while n >= 1024 and u < len(units)-1:
        n /= 1024.0; u += 1
    return f"{n:.2f}{units[u]}"

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <hdfs_path>")
    sys.exit(1)

path = sys.argv[1]

# count -q: 8 колонок, SPACE_CONSUMED здесь нет
out_count = subprocess.check_output(["hdfs","dfs","-count","-q",path], text=True).rstrip("\n")
parts = out_count.split()
if len(parts) < 8:
    raise RuntimeError(f"Unexpected -count output:\n{out_count}")
_,_,_,_, dirs, files, content, *rest = parts
dirs = int(dirs); files = int(files); content = int(content)
pathname = " ".join(rest) if rest else path

# du -s: size, space_consumed, path
out_du = subprocess.check_output(["hdfs","dfs","-du","-s",path], text=True).rstrip("\n")
parts_du = out_du.split()
if len(parts_du) < 3:
    raise RuntimeError(f"Unexpected -du output:\n{out_du}")
space_consumed = int(parts_du[1])

avg_factor = (space_consumed / content) if content > 0 else 0.0

print(f"Path              : {pathname}")
print(f"Dirs              : {dirs}")
print(f"Files             : {files}")
print(f"Logical size      : {human(content)}  (CONTENT_SIZE)")
print(f"Space consumed    : {human(space_consumed)}  (с учётом репликации/EC)")
print(f"Avg redundancy    : {avg_factor:.2f} x")