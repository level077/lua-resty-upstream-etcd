动态upstream，基于https://github.com/rrfeng/lua-resty-upstream-etcd 修改。   
整合了openresty自身的健康检查及lua-resty-balancer的rr，chash负载均衡算法。   
config.lua是etcd及健康检查的默认配置。

Usage
=================
nginx.conf相关配置如下：
```
http {
   lua_package_path "/path/to/resty/?.lua;;";
   #lua-resty-balancer需要
   lua_package_cpath "/path/to/resty/?.so;;";
   #worker启动时，从文件或者etcd中读取upstream配置
   init_worker_by_lua_file "/path/to/resty/init.lua";
   lua_shared_dict healthcheck 10m;

   #根据请求的host，做相关的upstream映射
   map $http_host $upstream {
       default  default.example.com;
       1.example.com 1.example.com;
       ......
   }

   upstream etcd_pool {
       server 0.0.0.1;
       #upstream的负载均衡
       balancer_by_lua_file "/path/to/resty/balancer.lua";
       keepalive 64;
   }
   ......
   server {
       ......
       location / {
	   #显示设置负载均衡算法为rr，默认为chash。只有chash，rr两种选择
	   set $hash_method "rr";
           ......
       }
   }
}
```

Methods
==================
find(upstream,key,hash_method)
------------------
```
local dyups = require "lua-resty-upstream-etcd.dyups"
local server = dyups.find(ngx.var.upstream,ngx.var.x_real_ip,ngx.var.hash_method)
```
参数:
```
upstream: upstream名字，如之前nginx.conf配置中设置的map
key: hash key
hash_method: 目前只有chash,rr两种
```

add_peer(upstream,param)
-------------------
```
local res, err = dyups.add_peer(upsteram,{host="127.0.0.1",port=80})
```
参数:
```
upstream: upsteam的名字
param: 一个table，必须包含的配置为host，port，status
```

delete_peer(upstream,param)
----------------------
```
local res, err = dyups.delete_peer(upstream,{host="127.0.0.1",port=80})
```
参数:
```
upstream: upstream名字
param: 一个table，必须包含的配置为host，port
```

delete_upstream(upstream)
--------------------------
```
local res, err = dyups.delete_upstream(upstream)
```
如果upstream下还有server，则必须先清空server，才能删除upstream

healthcheck(upstream,param)
--------------------------
```
local res, err = dyups.healthcheck(upstram,param)
```
参数:
```
upstream: upstream名字
param: 一个table，具体配置与openresty健康检查配置一致，详见https://github.com/openresty/lua-resty-upstream-healthcheck
```

status()
-----------------------
upstream状态信息及健康检查配置
```
dyups.status()
```

Dependencies
==========================
- https://github.com/agentzh/lua-resty-balancer
