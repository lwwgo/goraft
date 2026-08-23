SHELL := /bin/bash

# 在 Make 级别把 Go 的工具目录前置到 PATH，
# 解决 "go install 装完后 golangci-lint 依然 command not found" 的典型问题。
# GOBIN 优先级高于 GOPATH/bin，两者都带上。
export PATH := $(shell go env GOBIN):$(shell go env GOPATH)/bin:$(PATH)
GOPATH_BIN   := $(shell go env GOPATH)/bin

.PHONY: all build lint test demo clean

# ============ 可覆盖参数 ============
# build 产物输出目录（master 二进制在这里；demo 再从这里分发给各节点）
OUTPUT_DIR      ?= output
# demo 投票节点数(不含 learner)，3 是容忍单节点故障的最小奇数
DEMO_NODES      ?= 3
# 是否额外加 1 个 learner 节点 (0=否 1=是)
DEMO_WITH_LEARNER ?= 0
# demo 节点起始端口，后续节点依次 +1
DEMO_BASE_PORT  ?= 1231
# demo 产物目录
DEMO_DIR        ?= demo
# 二进制名字
BIN_NAME        ?= goraft
# master 二进制绝对路径（build 出的位置）
MASTER_BIN      := $(abspath $(OUTPUT_DIR)/bin/$(BIN_NAME))
# 生成 demo 的辅助脚本（纯 shell，无 Python）
GEN_DEMO_SH     := scripts/gen_demo.sh

# ============ 基础构建 ============
all: build

build:
	@mkdir -p $(OUTPUT_DIR)/bin
	go build -o $(OUTPUT_DIR)/bin/$(BIN_NAME) .
	@echo "[build] ok  ->  $(OUTPUT_DIR)/bin/$(BIN_NAME)"

# ============ lint ============
# 先找已安装的 golangci-lint：
#   1. 直接 PATH 里 command -v 找
#   2. 再退回到 $(GOPATH)/bin/golangci-lint 绝对路径找
# 都找不到再走 GOBIN=... go install 从源码安装（写用户目录，不触发 sandbox /bin 权限拒绝）
GOLANGCI_PATH := $(shell command -v golangci-lint 2>/dev/null)
ifeq ($(GOLANGCI_PATH),)
  GOLANGCI_PATH := $(shell [ -x $(GOPATH_BIN)/golangci-lint ] && echo $(GOPATH_BIN)/golangci-lint || echo)
endif

lint:
	@if [ -z "$(GOLANGCI_PATH)" ]; then \
		echo "[lint] golangci-lint not found, installing via go install (into $(GOPATH_BIN))..."; \
		GOBIN="$(GOPATH_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest; \
		GOLANGCI_BIN="$(GOPATH_BIN)/golangci-lint"; \
	else \
		GOLANGCI_BIN="$(GOLANGCI_PATH)"; \
	fi; \
	echo "[lint] using: $$GOLANGCI_BIN ($$($$GOLANGCI_BIN --version 2>&1 || echo 'unknown version'))"; \
	"$$GOLANGCI_BIN" run ./... ; \
	echo "[lint] pass"

# ============ test ============
HAS_TESTS := $(shell find . -name '*_test.go' -not -path './vendor/*' | head -n 1)

test:
	@echo "[test] go test -race -count=1 ./..."
	@if [ -z "$(HAS_TESTS)" ]; then \
		echo "  (no *_test.go found yet。可以在 server/、util/ 里开始写单元测试)"; \
	else \
		go test -race -count=1 ./... ; \
	fi
	@echo "[test] done"

# ============ demo ============
# 依赖 build：确保 output/bin 下有 master 二进制；然后 gen_demo.sh 只做分发（cp 到各节点各自的 bin/）
demo: build
	@echo "[demo] nodes=$(DEMO_NODES) with_learner=$(DEMO_WITH_LEARNER) base_port=$(DEMO_BASE_PORT)"
	@echo "[demo] master bin source: $(MASTER_BIN)"
	@bash $(GEN_DEMO_SH) \
		"$(DEMO_DIR)" \
		"$(DEMO_NODES)" \
		"$(DEMO_BASE_PORT)" \
		"$(DEMO_WITH_LEARNER)" \
		"$(MASTER_BIN)"

# ============ clean ============
clean:
	rm -rf $(OUTPUT_DIR) $(DEMO_DIR)
	@echo "[clean] removed $(OUTPUT_DIR)/ $(DEMO_DIR)/"
