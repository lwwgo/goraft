#!/usr/bin/env bash
# 生成 goraft demo 目录结构（纯 shell，无外部依赖）。
# 用法:
#   ./scripts/gen_demo.sh <out_dir> <demo_nodes> <base_port> <with_learner> <bin_abs_path>
#
# 示例:
#   ./scripts/gen_demo.sh ./demo 3 1231 0 $PWD/demo/bin/goraft
set -euo pipefail

if [ "$#" -ne 5 ]; then
  echo "usage: $0 <out_dir> <demo_nodes> <base_port> <with_learner> <bin_abs_path>" >&2
  exit 2
fi

OUT_DIR="$(cd "$1" 2>/dev/null && pwd || (mkdir -p "$1" && cd "$1" && pwd))"
DEMO_NODES="$2"
BASE_PORT="$3"
WITH_LEARNER="$4"
BIN_PATH="$5"

if [ ! -x "$BIN_PATH" ]; then
  echo "error: master binary not found or not executable: $BIN_PATH" >&2
  echo "       (run make build first, or pass an existing bin as 5th arg)" >&2
  exit 1
fi

# ============ 0. 基础目录 ============
# master 二进制在 output/ 下（跟 demo/ 是分开的同级目录）；
# 每个节点自己的 log / wal / snapshot / bin 都在 <node>/ 子目录里, 所以 demo/ 根下不再需要独立的 log/bin 目录。
rm -rf "$OUT_DIR/bin" "$OUT_DIR/log" "$OUT_DIR/start.sh" "$OUT_DIR/.pids"
for old in "$OUT_DIR"/node* "$OUT_DIR"/learner; do
  [ -e "$old" ] || continue
  rm -rf "$old"
done

# ============ 1. 生成 voter peers(全体 voter 地址列表, 供每个节点生成 conf 时按自己剔除) ============
# 用 bash 数组存 voter 地址列表: voter_addrs[i] = 第 i 个 voter 的 addr
declare -a voter_addrs=()
for ((i=0; i<DEMO_NODES; i++)); do
  voter_addrs+=("127.0.0.1:$((BASE_PORT + i))")
done

LEARNER_ADDR=""
if [ "$WITH_LEARNER" = "1" ]; then
  LEARNER_ADDR="127.0.0.1:$((BASE_PORT + DEMO_NODES))"
fi

# ============ 2. 写单个节点 conf.toml 并拷贝独立二进制 ============
# write_conf <node_name> <local_addr> <is_learner>
write_conf() {
  local node_name="$1"
  local local_addr="$2"
  local is_learner="$3"
  local node_dir="$OUT_DIR/$node_name"

  rm -rf "$node_dir"
  mkdir -p "$node_dir/conf" "$node_dir/log" "$node_dir/wal" "$node_dir/snapshot" "$node_dir/bin"

  local wal_abs snap_abs log_abs
  wal_abs="$(cd "$node_dir/wal" && pwd)"
  snap_abs="$(cd "$node_dir/snapshot" && pwd)"
  log_abs="$(cd "$node_dir/log" && pwd)"

  # 给该节点组装 peers: 所有 voter - 自己 (实现里 Peers 语义 = "集群中其他节点", 含自己会抬高多数派分母)
  local peers=""
  local addr
  for addr in "${voter_addrs[@]}"; do
    [ "$addr" = "$local_addr" ] && continue
    if [ -z "$peers" ]; then
      peers="\"$addr\""
    else
      peers="$peers,\"$addr\""
    fi
  done

  local is_learner_str="false"
  [ "$is_learner" = "1" ] && is_learner_str="true"

  local learner_field
  if [ -z "$LEARNER_ADDR" ]; then
    learner_field='learner = ""'
  else
    learner_field="learner = \"$LEARNER_ADDR\""
  fi

  cat > "$node_dir/conf/conf.toml" <<EOF
localID = "$local_addr"
isLearner = $is_learner_str
peers = [$peers]
$learner_field
walDir = "$wal_abs"
snapDir = "$snap_abs"
maxIndexSpan = 10000
logDir = "$log_abs"
EOF

  # 每个节点一份独立二进制, 重命名成 goraft-<node_name>, 这样 `ps` 能区分 cmd
  local node_bin="$node_dir/bin/goraft-$node_name"
  cp "$BIN_PATH" "$node_bin"
  chmod +x "$node_bin"
  echo "  -> $node_name conf ($local_addr) -> $node_dir/conf/conf.toml"
  echo "     local peers (不含自己)  = [$peers]"
  echo "     bin -> $node_bin"
}

echo "[gen] writing $DEMO_NODES voter node configs..."
for ((i=0; i<DEMO_NODES; i++)); do
  write_conf "node$i" "127.0.0.1:$((BASE_PORT + i))" 0
done

if [ "$WITH_LEARNER" = "1" ]; then
  echo "[gen] writing learner config..."
  write_conf "learner" "$LEARNER_ADDR" 1
fi

# ============ 3. 生成 start.sh ============
START_SH="$OUT_DIR/start.sh"
# 先写带占位符的模板，再用 sed 替换为实际值（兼容 mac/gnu sed）
cat > "$START_SH" <<'TEMPLATE_EOF'
#!/usr/bin/env bash
# goraft demo 启动脚本
# 用法:
#   ./start.sh          前台启动, Ctrl+C 一键停所有节点
#   ./start.sh -d       后台启动, 用 ./start.sh stop 停止
#   ./start.sh stop     停止所有由本脚本启动的节点
#   ./start.sh status   查看每个节点存活状态和推测的角色
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.pids"
DEMO_NODES="__DEMO_NODES__"
BASE_PORT="__BASE_PORT__"
WITH_LEARNER="__WITH_LEARNER__"

# 根据节点名拿到该节点自己那份独立重命名后的二进制(便于 ps 区分不同节点的 cmd)
_node_bin() {
  local name="$1"
  echo "$SCRIPT_DIR/$name/bin/goraft-$name"
}

# 列出所有节点名
_list_nodes() {
  local i
  for ((i=0; i<DEMO_NODES; i++)); do
    echo "node$i"
  done
  if [ "$WITH_LEARNER" = "1" ]; then
    echo "learner"
  fi
}

# 根据节点名返回监听端口
_node_port() {
  local name="$1"
  if [[ "$name" == "learner" ]]; then
    echo $((BASE_PORT + DEMO_NODES))
  else
    echo $((BASE_PORT + ${name#node}))
  fi
}

# 启动一个节点，输出 pid
_start_one() {
  local node="$1"
  local node_dir="$SCRIPT_DIR/$node"
  local node_bin
  node_bin="$(_node_bin "$node")"
  mkdir -p "$node_dir/log"
  ( cd "$node_dir" && exec nohup "$node_bin" -c conf > "$node_dir/log/start.stdout.log" 2>&1 &
    echo $! )
}

# 启动所有节点
_start_all() {
  local mode="${1:-foreground}"
  : > "$PID_FILE"
  local learners
  learners=$( [ "$WITH_LEARNER" = "1" ] && echo 1 || echo 0 )
  echo "[start] launching $DEMO_NODES followers + $learners learner via Raft election..."

  local -a pids=()
  local node pid port
  while read -r node; do
    [ -z "$node" ] && continue
    pid=$(_start_one "$node")
    echo "$node $pid" >> "$PID_FILE"
    pids+=("$pid")
    port=$(_node_port "$node")
    printf "    %-12s pid=%-7s port=%s\n" "$node" "$pid" "$port"
  done < <(_list_nodes)

  echo "[start] all up. 选举超时默认 10s 左右, 耐心等待一轮就会产生 leader."
  echo "[start] 每个节点日志路径: $SCRIPT_DIR/<node>/log/running.log"

  if [ "$mode" = "foreground" ]; then
    _cleanup() {
      echo
      echo "[stop] caught signal, killing all nodes..."
      kill "${pids[@]}" 2>/dev/null || true
      rm -f "$PID_FILE"
      exit 0
    }
    trap _cleanup INT TERM
    wait
  fi
}

_stop_all() {
  if [ ! -f "$PID_FILE" ]; then
    echo "[stop] pid file not found, nothing to stop"
    return 0
  fi
  local node pid
  while read -r node pid _ || [ -n "$node" ]; do
    [ -z "${pid:-}" ] && continue
    if kill -0 "$pid" 2>/dev/null; then
      echo "  -> killing $node pid=$pid"
      kill "$pid" 2>/dev/null || true
    else
      echo "  -> $node pid=$pid already stopped"
    fi
  done < "$PID_FILE"
  rm -f "$PID_FILE"
  echo "[stop] done"
}

# 从 running.log 猜最近的角色变更
_infer_role() {
  local log_file="$1"
  [ -s "$log_file" ] || { echo "?"; return; }
  local last
  last=$(grep -oE 'become to be leader|change state from [a-z]+ to [a-z]+' "$log_file" | tail -n 1 || true)
  if [[ "$last" == *"become to be leader"* ]]; then
    echo "LEADER"
  elif [[ "$last" =~ to\ ([a-z]+)$ ]]; then
    echo "${BASH_REMATCH[1]^^}"
  else
    echo "FOLLOWER"
  fi
}

_status() {
  echo "=== goraft demo status ==="
  if [ ! -f "$PID_FILE" ]; then
    echo "  (not running: $0 [-d] 启动)"
    return 0
  fi
  local node pid port alive role log_file
  while read -r node pid _ || [ -n "$node" ]; do
    [ -z "${pid:-}" ] && continue
    port=$(_node_port "$node")
    alive="DEAD"
    role="?"
    if kill -0 "$pid" 2>/dev/null; then
      alive="ALIVE"
      log_file="$SCRIPT_DIR/$node/log/running.log"
      role=$(_infer_role "$log_file")
    fi
    printf "  %-12s pid=%-7s port=%-5s %-6s role=%s\n" "$node" "$pid" "$port" "$alive" "$role"
  done < "$PID_FILE"
}

case "${1:-foreground}" in
  -d|daemon)   _start_all background ;;
  stop)        _stop_all ;;
  status)      _status ;;
  foreground)  _start_all foreground ;;
  *)           echo "usage: $0 [-d|daemon|stop|status]"; exit 1 ;;
esac
TEMPLATE_EOF

# 替换占位符（兼容 mac sed 的 -i 语法）
sed -i.bak \
  -e "s|__DEMO_NODES__|${DEMO_NODES}|" \
  -e "s|__BASE_PORT__|${BASE_PORT}|" \
  -e "s|__WITH_LEARNER__|${WITH_LEARNER}|" \
  "$START_SH"
rm -f "${START_SH}.bak"
chmod +x "$START_SH"

echo "[gen] start.sh -> $START_SH"
echo
echo "[gen] usage:"
echo "  $START_SH          前台启动 (Ctrl+C 停)"
echo "  $START_SH -d       后台启动"
echo "  $START_SH stop     停止"
echo "  $START_SH status   查看状态/角色"
