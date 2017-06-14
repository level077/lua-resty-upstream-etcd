local dyups = require("lua-resty-upstream-etcd.dyups")
local cjson = require "cjson"
local args = ngx.req.get_uri_args()
local t = {}
if args["concurrency"] and args["concurrency"] ~= "" then
        t.concurrency = args["concurrency"]
end
if args["valid_statuses"] and args["valid_statuses"] ~= "" then
        t.valid_statuses = {args["valid_statuses"]}
end
if args["typ"] and args["typ"] ~= "" then
        t.typ = args["typ"]
end
if args["http_req"] and args["http_req"] ~= "" then
        t.http_req = ngx.escape_uri(args["http_req"])
end
if args["fall"] and args["fall"] ~= "" then
        t.fall = args["fall"]
end
if args["rise"] and args["rise"] ~= "" and tonumber(args["rise"])then
        t.rise = args["rise"]
end
if args["timeout"] and args["timeout"] ~= "" and tonumber(args["timeout"]) then
        t.timeout = args["timeout"]
end
if args["interval"] and args["interval"] ~= "" and tonumber(args["interval"]) then
        t.interval = args["interval"]
end
local res, err = dyups.healthcheck(args["upstream"],t)
if not res then
        ngx.say(err)
else
        ngx.say(cjson.encode(res))
end
