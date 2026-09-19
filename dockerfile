# syntax=docker/dockerfile:1.7
#
# 多架构构建策略：前端与 Go 编译阶段固定在构建机原生架构上运行（--platform=$BUILDPLATFORM），
# Go 通过 GOOS/GOARCH 交叉编译（CGO_ENABLED=0），只有最后的运行层才是目标架构，避免 QEMU 模拟编译。
# BuildKit 缓存挂载：pnpm store、Go module、go-build。

FROM --platform=$BUILDPLATFORM node:25.2.1 AS web_builder

RUN npm install -g pnpm@10

WORKDIR /web
COPY web/package.json web/pnpm-lock.yaml ./

RUN --mount=type=cache,id=pnpm-store,target=/pnpm/store \
    pnpm install --frozen-lockfile --shamefully-hoist --store-dir /pnpm/store

COPY web/ ./
RUN pnpm run build:prod

FROM --platform=$BUILDPLATFORM golang:1.26.2-alpine3.23 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=unknown

ENV GO111MODULE=on \
    CGO_ENABLED=0

WORKDIR /go/release

# 先下载依赖，源码变化时复用依赖层
COPY go.mod go.sum ./
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod go mod download

COPY . .
COPY --from=web_builder /web/dist ./static/secure

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild-${TARGETOS}-${TARGETARCH},target=/root/.cache/go-build \
    set -x \
    && MODULE_PATH=$(go list -m) \
    && GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags="-X '${MODULE_PATH}/app.Version=${VERSION}' -s -w -buildid=" \
    -o bepusdt ./main

FROM alpine:3.20

ENV TZ=Asia/Shanghai

# 安装所需的依赖
RUN apk add --no-cache tzdata ca-certificates

COPY --from=builder /go/release/bepusdt /usr/local/bin/bepusdt

# 设置时区
RUN ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

EXPOSE 8080
ENTRYPOINT ["bepusdt"]
CMD ["start"]
