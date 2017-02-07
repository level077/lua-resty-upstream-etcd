local _M = {}
local http = require "lua-resty-http.http"
local json = require "cjson"

local ngx_timer_at = ngx.timer.at
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_INFO = ngx.INFO
local ngx_sleep = ngx.sleep
local ngx_worker_exiting = ngx.worker.exiting

_M.ready = false
_M.data = {}

local function length(T)
        local count = 0
        for _ in pairs(T) do count = count + 1 end
        return count
end

local function log(...)
    ngx_log(ngx_ERR, ...)
end

local function log_info(...)
    ngx_log(ngx_INFO, ...)
end

local function copyTab(st)
    local tab = {}
    for k, v in pairs(st or {}) do
        if type(v) ~= "table" then
            tab[k] = v
        else
            tab[k] = copyTab(v)
        end
    end
    return tab
end

local function indexof(t, e)
    for k, v in pairs(t) do
        if v.host == e.host and v.port == e.port then
            return k
        end
    end
    return nil
end

local function basename(s)
    local x, y = s:match("(.*)/([^/]*)/?")
    return y, x
end

local function split_addr(s)
    host, port = s:match("(.*):([0-9]+)")

    -- verify the port
    local p = tonumber(port)
    if p == nil then
        return "127.0.0.1", 0, "port invalid"
    elseif p < 1 or p > 65535 then
        return "127.0.0.1", 0, "port invalid"
    end

    -- verify the ip addr
    local chunks = {host:match("(%d+)%.(%d+)%.(%d+)%.(%d+)")}
    if (#chunks == 4) then
        for _,v in pairs(chunks) do
            if (tonumber(v) < 0 or tonumber(v) > 255) then
                return "127.0.0.1", 0, "host invalid"
            end
        end
    else
        return "127.0.0.1", 0, "host invalid"
    end

    -- verify pass
    return host, port, nil
end

local function get_lock()
    local dict = _M.conf.dict
    local key = "lock"
    -- only the worker who get the lock can update the dump file.
    local ok, err = dict:add(key, true)
    if not ok then
        if err == "exists" then
            return nil
        end
        log("failed to add key \"", key, "\": ", err)
        return nil
    end
    return true
end

local function release_lock()
    local dict = _M.conf.dict
    local key = "lock"
    local ok, err = dict:delete(key)
    return true
end

local function dump_tofile(force)
    local cur_v = _M.data.version
    local saved = false
    local dict = _M.conf.dict
    while not saved do
        local pre_v = dict:get("version")
        if not force then
            if pre_v then
                if tonumber(pre_v) >= tonumber(cur_v) then
                    return true
                end
            end
        end

        local l = get_lock()
        if l then
            local f_path = _M.conf.dump_file .. _M.conf.etcd_path:gsub("/", "_")
            local file, err = io.open(f_path, 'w')
            if file == nil then
                log("Can't open file: " .. f_path .. err)
                release_lock()
                return false
            end

            local data = json.encode(_M.data)
            file:write(data)
            file:flush()
            file:close()

            dict:set("version", cur_v)
            saved = true
            release_lock()
        else
            ngx_sleep(0.2)
        end
    end
end

local function set_healthcheck(upstream,value)
        local check
        if not _M.data[upstream] then
                _M.data[upstream] = {}
        end
        if _M.data[upstream].healthcheck then
                check = _M.data[upstream].healthcheck
        end
        local check_type, check_req, check_interval, check_timeout, check_rise, check_concurrency, check_fall, check_valid
        if not check then
                check_type = _M.conf.check_type or "http"
                check_req = _M.conf.check_req or "GET /1.htm HTTP/1.0\r\nHost: foo.com\r\n\r\n"
                check_interval = _M.conf.check_interval or 3000
                check_timeout = _M.conf.check_timeout or 2000
                check_rise = _M.conf.check_rise or 2
                check_concurrency = _M.conf.check_concurrency or 10
                check_fall = _M.conf.check_fall or 3
                check_valid = _M.conf.check_valid or {200}
        else
                if check["typ"] then
                        check_type = check["typ"]
                end
                if check["http_req"] then
                        check_req = check["http_req"]
                end
                if check["interval"] then
                        check_interval = check["interval"]
                end
                if check["timeout"] then
                        check_timeout = check["timeout"]
                end
                if check["rise"] then
                        check_rise = check["rise"]
                end
                if check["concurrency"] then
                        check_concurrency = check["concurrency"]
                end
		if check["fall"] then
                        check_fall = check["fall"]
                end
                if check["valid_statuses"] then
                        check_valid = check["valid_statuses"]
                end
        end
        if value.type then
                check_type = value.type
        end
        if value.http_req then
                check_req = value.http_req
        end
        if value.interval then
                check_interval = value.interval
        end
        if value.timeout then
                check_timeout = value.timeout
        end
        if value.rise then
                check_rise = value.rise
        end
        if value.fall then
                check_fall = value.fall
        end
        if value.concurrency then
                check_concurrency = value.concurrency
        end
        if value.valid_statuses then
                check_valid = value.valid_statuses
        end
        _M.data[upstream].healthcheck = {typ=check_type,http_req=check_req,interval=check_interval,timeout=check_timeout,rise=check_rise,concurrency=check_concurrency,valid_statuses = check_valid,fall = check_fall}
end

local function spawn_healthcheck(upstream)
        local hc = require "lua-resty-upstream-etcd.healthcheck"
        if not _M.data[upstream].healthcheck then
                set_healthcheck(upstream,{})
                log_info("upstream:".. upstream .." use default healthcheck")
        end
        local check = _M.data[upstream].healthcheck
        local ok, err = hc.spawn_checker{
                dict = _M.conf.dict,
                upstream = upstream,
                type = check.typ,
                http_req = check.http_req,
                interval = check.interval,
                timeout = check.timeout,
                rise = check.rise,
                fall = check.fall,
                valid_statuses = check.valid_statuses,
                concurrency = check.concurrency,
        }
        return ok, err
end

local function hash_server(name)
        local resty_chash = require "lua-resty-balancer.chash"
        local resty_rr = require "lua-resty-balancer.roundrobin"
        local server_list = _M.data[name].up_servers
        local count = length(server_list)
        if count == 0 then
                _M[name] = nil
                log("upstream:",name," not have a node")
                return
        end
        if not _M[name] then
                _M[name] = {}
                local err
                _M[name]["chash"], err = resty_chash:new(server_list)
                if not _M[name]["chash"] then
                        log("chash init err:", err)
                end
                _M[name]["rr"], err = resty_rr:new(server_list)
                if not _M[name]["rr"] then
                        log("rr init err:", err)
                end
                log_info("balancer init:" .. name)
        else
                _M[name]["chash"]:reinit(server_list)
                log_info("chash reinit:" .. name)
                _M[name]["rr"]:reinit(server_list)
                log_info("rr reinit:" .. name)
        end
end

local function spawn_healthcheck_from_file()
        for k,v in pairs(_M.data) do
                if type(v) == "table" then
                        local ok, err = spawn_healthcheck(k)
                        if not ok then
                                log(err)
                        else
                                log_info("start watch from file:",k)
                        end
                        hash_server(k)
                        log_info("hash " .. k .. " due to start from file")
                end
        end
end

function _M.find(name,key,hash_method)
	if not _M[name] then
		log("upstream:",name," haven't hash")
		return nil, "upstream:" .. name .. " haven't hash"
	end
	local method = hash_method
	if not method or (method ~= "chash" and method ~= "rr") then
		log("invalid hash method:", method," use default hash method: chash")
		method = "chash"
	end
	local hash = _M[name][method]
	if not hash then
		log("can't find ",name,"'s hash handle:",method)	
		return nil, "can't find " .. name .. "'s hash handle:" .. method
	end
	return hash:find(key)
end

local function watch(premature, conf, index)
    if premature then
        return
    end

    if ngx_worker_exiting() then
        return
    end

    local c = http:new()
    c:set_timeout(120000)
    c:connect(conf.etcd_host, conf.etcd_port)

    local nextIndex
    local url = "/v2/keys" .. conf.etcd_path

    -- First time to init all the upstreams.
    if index == nil then
        local s_url = url .. "?recursive=true"
        local res, err = c:request({ path = s_url, method = "GET" })
        if not err then
            local body, err = res:read_body()
            if not err then
                local all = json.decode(body)
                if not all.errorCode and all.node.nodes then
                    for n, s in pairs(all.node.nodes) do
                        local name = basename(s.key)
                        _M.data[name] = { servers={}, up_servers={}}
                        local s_url = url .. "/" .. name .. "?recursive=true"
                        local res, err = c:request({path = s_url, method = "GET"})
                        if not err then
                            local body, err = res:read_body()
                            if not err then
                                local svc = json.decode(body)
                                if not svc.errorCode and svc.node.nodes then
                                    for i, j in pairs(svc.node.nodes) do
                                        local w = 1
                                        local s = "down"
                                        local b = basename(j.key)
                                        local ok, value = pcall(json.decode, j.value)

                                        if type(value) == "table" then
                                            if value.weight then
                                                w = value.weight
                                            end
                                            if value.status then
                                                s = value.status
                                            end
                                        end

                                        local h, p, err = split_addr(b)
                                        if not err then
                                            _M.data[name].servers[#_M.data[name].servers+1] = {host=h, port=p, weight=w, status=s}
					    if s == "up" then
						_M.data[name].up_servers[h..":"..p] = w
					    end
                                        end
					if b == "healthcheck" then
						set_healthcheck(name,value)
					end
                                    end
                                end
                            end
                            _M.data.version = res.headers["x-etcd-index"]
                        end
			local ok, err = spawn_healthcheck(name)
			if not ok then
				log(err)
			end	
			log_info("start init healthcheck:",name)
			hash_server(name)
			log_info("chash " .. name .. " due to start from etcd")
                    end
                end
                _M.ready = true
                if _M.data.version then
                    nextIndex = _M.data.version + 1
                end
                dump_tofile(true)
            end
        end

    -- Watch the change and update the data.
    else
        local s_url = url .. "?wait=true&recursive=true&waitIndex=" .. index
        local res, err = c:request({ path = s_url, method = "GET" })
        if not err then
            local body, err = res:read_body()
            if not err then
                -- log("DEBUG: recieve change: "..body)
                local change = json.decode(body)

                if not change.errorCode then
                    local action = change.action
                    if change.node.dir then
                        local target = change.node.key:match(_M.conf.etcd_path .. '/(.*)/?')
                        if action == "delete" then
                            _M.data[target] = nil
			    log_info("DELETE [".. target .. "]")
                        elseif action == "set" or action == "update" then
                            local new_svc = target:match('([^/]*).*')
                            if not _M.data[new_svc] then
                                _M.data[new_svc] = {servers={}, up_servers={}}
				log_info("ADD [".. new_svc .. "]")
				local ok, err = spawn_healthcheck(new_svc)
                        	if not ok then
					log(err)
                        	end
                        	log_info("start dir watch healthcheck:" .. new_svc)
                            end
                        end
                    else
                        local bkd, ret = basename(change.node.key)
                        local ok, value = pcall(json.decode, change.node.value)

                        local w = 1
                        local s = "down"

                        if type(value) == "table" then
                            if value.weight then
                                w = value.weight
                            end
                            if value.status then
                                s = value.status
                            end
                        end

                        local h, p, err = split_addr(bkd)
                        if not err then
                            local bs = {host=h, port=p, weight=w, status=s}
                            local svc = basename(ret)

                            if action == "delete" or action == "expire" then
                                table.remove(_M.data[svc].servers, indexof(_M.data[svc].servers, bs))
				if _M.data[svc].up_servers[h..":"..p] then
					_M.data[svc].up_servers[h..":"..p] = nil
					log_info("DELETE up_servers [".. svc .. "]: " .. h .. ":" .. p)
					hash_server(svc)
					log_info("chash " .. svc .. " due to DELETE " .. h .. ":" .. p)
				end
                                log_info("DELETE [".. svc .. "]: " .. bs.host .. ":" .. bs.port)
                            elseif action == "set" or action == "update" then
                                if not _M.data[svc] then
                                    _M.data[svc] = {servers={bs},up_servers={}}
				    log_info("ADD [" .. svc .. "]: " .. bs.host ..":".. bs.port)
				    if s == "up" then
					_M.data[svc].up_servers[h..":"..p] = w
					log_info("ADD up_servers [" .. svc .. "]: " .. h ..":".. p)
					hash_server(svc)
					log_info("hash " .. svc .. " due to DIR ADD " .. h .. ":" .. p)
				    end
				    local ok, err = spawn_healthcheck(svc)
                        	    if not ok then
                                	log(err)
				    else
					log_info("start healthcheck due to DIR ADD:" .. svc)
                        	    end
                                else
                                    local index = indexof(_M.data[svc].servers, bs)
                                    if index == nil then
                                        log_info("ADD [" .. svc .. "]: " .. bs.host ..":".. bs.port)
                                        table.insert(_M.data[svc].servers, bs)
					if s == "up" then
						_M.data[svc].up_servers[h..":"..p] = w
						log_info("ADD up_servers [" .. svc .. "]: " .. h ..":".. p)
						hash_server(svc)
						log_info("hash " .. svc .. " due to KEY ADD " .. h .. ":" .. p)
					end
                                    else
                                        _M.data[svc].servers[index] = bs
                                        log_info("MODIFY [" .. svc .. "]: " .. bs.host ..":".. bs.port .. " " .. change.node.value)
					if s == "up" then
						_M.data[svc].up_servers[h..":"..p] = w
						log_info("MODIFY ADD up_servers [" .. svc .. "]: " .. h ..":".. p)
						hash_server(svc)
						log_info("hash " .. svc .. " due to MODIFY " .. h .. ":" .. p)
					elseif s == "down" then
						if _M.data[svc].up_servers[h..":"..p] then
							_M.data[svc].up_servers[h..":"..p] = nil
							log_info("MODIFY DELETE up_servers [" .. svc .. "]: " .. h ..":".. p)
							hash_server(svc)
							log_info("hash " .. svc .. " due to DELETE " .. h .. ":" .. p)
						end
					end
                                    end
                                end
                            end
                        else
			    if bkd == "healthcheck" then
                            	set_healthcheck(basename(ret),value)
				log_info("update healthcheck,","ret:",ret)
			    else
                            	log("bkd:",bkd," err:",err)
			    end
                        end
                    end
                    _M.data.version = change.node.modifiedIndex
                    nextIndex = _M.data.version + 1
                elseif change.errorCode == 401 then
                    nextIndex = nil
                end
            elseif err == "timeout" then
                nextIndex = res.headers["x-etcd-index"] + 1
            end
            dump_tofile(false)
        end
    end
    c:close()

    -- Start the update cycle.
    local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
    if not ok then
        log("Error start watch: ", err)
    end
    return
end

function _M.init()
    local conf = require "lua-resty-upstream-etcd.config"
    if not conf then
	log("require config.lua")
	return
    end
    -- Load the upstreams from file
    local s = ngx.re.match(conf.etcd_path,'(^/.*?)/{0,}$',"jo")
    if not s then
       log("etcd_path config error")
       return
    else
       conf.etcd_path = s[1]
    end
    if not _M.ready then
        _M.conf = conf
        local f_path = _M.conf.dump_file .. _M.conf.etcd_path:gsub("/", "_")
        local file, err = io.open(f_path, "r")
        if file == nil then
            log(err)
            local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
            if not ok then
                log("Error start watch: " .. err)
            end
            return
        else
            local d = file:read("*a")
            local data = json.decode(d)
            if err then
                log(err)
                local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
                if not ok then
                    log("Error start watch: " .. err)
                end
                return
            else
                _M.data = copyTab(data)
                file:close()
                _M.ready = true
                if _M.data.version then
                    nextIndex = _M.data.version + 1
		    spawn_healthcheck_from_file()
                end
            end
        end
    end

    -- Start the etcd watcher
    local ok, err = ngx_timer_at(0, watch, conf, nextIndex)
    if not ok then
        log("Error start watch: " .. err)
    end 
end

function _M.status()
	for k,v in pairs(_M.data)
	do
		if type(v) == "table" then
			ngx.say("Upstream " .. k)
			if v["servers"] then
				for m,n in pairs(v["servers"])
				do
					ngx.say("    " ..  n["host"] .. ":" .. n["port"] .. " " .. n["status"])
				end
			end
			ngx.say("    healthcheck:",json.encode(v["healthcheck"]))
		end
	end
end

function _M.get_check_conf(upstream)
	if not _M.data[upstream] then
		return nil
	end
	if _M.data[upstream].healthcheck then
		return _M.data[upstream].healthcheck
	else
		return nil
	end 
end

function _M.get_primary_peers(upstream)
	if _M.data[upstream] then
		return _M.data[upstream].servers
	else
		return nil
	end
end

local function request_etcd(param)
	if type(param) ~= "table" then
		return nil, "param must be table"
	end
	local conf = _M.conf
	local c = http:new()
	c:set_timeout(20000)
	c:connect(conf.etcd_host, conf.etcd_port)
	local res, err = c:request(param)
	if not err then
		local body, err = res:read_body()
		if not err then
			local all = json.decode(body)
			if all.errorCode then
				return nil, body
			else
				return true
			end
		else
			return nil, err	
		end
	else
		return nil, err
	end
end

function _M.set_peer_down(upstream,server)
	local conf = _M.conf
	local url = "/v2/keys" .. conf.etcd_path .. "/" .. upstream .. "/" .. server.host .. ":" .. server.port
	return request_etcd({path = url, method = "PUT", body = "value=" .. json.encode(server), headers = {["Content-Type"] = "application/x-www-form-urlencoded"}})
end

function _M.delete_peer(upstream,server)
	local conf = _M.conf
	if type(server) ~= "table" then
                return nil, "server must be table"
        end
	local url = "/v2/keys" .. conf.etcd_path .. "/" .. upstream .. "/" .. server.host .. ":" .. server.port
	return request_etcd({ path = url, method = "DELETE"})
	
end

function _M.delete_upstream(upstream)
	local conf = _M.conf
	if not _M.data[upstream] then
		return nil, upstream .. "not exist"
	elseif length(_M.data[upstream].servers) > 0 then
		return nil, upstream .. " not empty"
	else
		local url = "/v2/keys" .. conf.etcd_path .. "/" .. upstream .. "?recursive=true"
                return request_etcd({path=url, method = "DELETE"})
	end
end

function _M.add_peer(upstream,server)
	if type(server) ~= "table" then
		return nil, "server must be table"
	end
	return _M.set_peer_down(upstream,server)
end

function _M.healthcheck(upstream,health_conf)
	local conf = _M.conf
	if type(health_conf) ~= "table" then
                return nil, "health_conf must be table"
        end
        local url = "/v2/keys" .. conf.etcd_path .. "/" .. upstream .. "/healthcheck"
	return request_etcd({ path = url, method = "PUT", body = "value="..json.encode(health_conf), headers = {["Content-Type"] = "application/x-www-form-urlencoded"} })
end

return _M
