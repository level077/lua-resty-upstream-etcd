local _M = {
	etcd_host = '127.0.0.1',
	etcd_port = 2379,
	etcd_path = '/upstream',
	dump_file = "/tmp/nginx",
	dict = ngx.shared.healthcheck,
	check_type = "http",
	check_req = "GET /test/1.htm HTTP/1.0\r\nHost: foo.com\r\n\r\n",
	check_interval = 3000,
	check_timeout = 2000,
	check_rise = 2,
	check_concurrency = 10,
	check_fall = 3,
	check_valid = {200,302},
}

return _M
