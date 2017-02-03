local u = require "lua-resty-upstream-etcd.dyups"
u.init({
	etcd_host = "127.0.0.1",
	etcd_port = 2379,
	etcd_path = "/upstream/",
	dump_file = "/tmp/nginx",
	dict = ngx.shared.healthcheck,
})
