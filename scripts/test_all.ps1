# infer-router 全量测试入口：vet + build + 单测/集成（可选 -race，需本机有 gcc）
# 用法：powershell scripts\test_all.ps1
$ErrorActionPreference = "Stop"

# Go 定位：GO_BIN 环境变量 > PATH > 便携版默认路径
$go = $env:GO_BIN
if (-not $go) {
    if (Get-Command go -ErrorAction SilentlyContinue) { $go = "go" }
    else { $go = "C:\Users\85473\Tools\go\bin\go.exe" }
}
Write-Output "== go: $go"
& $go version
if ($LASTEXITCODE -ne 0) { throw "go not available" }

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    if (-not $env:GOPROXY) { $env:GOPROXY = "https://goproxy.cn,direct" }

    Write-Output "== go vet ./..."
    & $go vet ./...
    if ($LASTEXITCODE -ne 0) { throw "vet failed" }

    Write-Output "== go build ./..."
    & $go build ./...
    if ($LASTEXITCODE -ne 0) { throw "build failed" }

    if (Get-Command gcc -ErrorAction SilentlyContinue) {
        Write-Output "== go test -race ./..."
        & $go test -race ./...
    } else {
        Write-Output "== go test ./...  (skip -race: gcc not found on this host)"
        & $go test ./...
    }
    if ($LASTEXITCODE -ne 0) { throw "tests failed" }
    Write-Output "== ALL GREEN"
} finally {
    Pop-Location
}
