local b = require "ngx.balancer"
local dyups = require "lua-resty-upstream-etcd.dyups"
local server = dyups.find(ngx.var.upstream,ngx.var.remote_addr,ngx.var.hash_method)
local ok, err = b.set_current_peer(server)
if not ok then
	ngx.log(ngx.ERR,err)
end
