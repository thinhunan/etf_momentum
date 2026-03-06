# 推送到 GitHub

## 1. 在 GitHub 上创建仓库

1. 打开：https://github.com/new?name=etf_momentum  
2. Repository name 保持 **etf_momentum**，Public，不要勾选 “Add a README”  
3. 点击 **Create repository**

## 2. 使用代理并推送（SSH）

在终端执行前先开代理（如 Clash 等），然后：

```bash
# 按你的代理端口修改（常见 7890 / 7891 / 1080）
export https_proxy=http://127.0.0.1:7890
export http_proxy=http://127.0.0.1:7890
export all_proxy=socks5://127.0.0.1:7890

cd /Users/Think/Documents/code/bigrich/yfinance
/usr/bin/git push -u origin master
```

若 SSH 无法直连 GitHub，需在 `~/.ssh/config` 里为 GitHub 配置代理，例如：

```
Host github.com
  HostName github.com
  User git
  ProxyCommand nc -X 5 -x 127.0.0.1:7890 %h %p
```

推送时如提示输入 SSH 密钥密码（passphrase），输入即可。

## 3. 首次推送后

GitHub 默认分支可能是 `main`。若远程是空仓库，上面命令会把本地 `master` 推上去。若之后想改为 `main`：

```bash
git branch -M main
/usr/bin/git push -u origin main
```
