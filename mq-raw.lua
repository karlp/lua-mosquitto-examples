#!/usr/bin/lua
--[[
Karl Palsson, 2016 <karlp@tweak.net.au>
Simplest possible lua-mosquitto example.
(well, we use penlight too, but you should just do that all the time...)
]]

local mosq = require("mosquitto")
mosq.init()

local pl = require("pl.import_into")()
-- intellij lua plugin quirks in my install
local printf = function(...)
    pl.utils.fprintf(io.stderr, ...)
    --pl.utils.printf(...)
end
local args = pl.lapp [[
    -H,--host (default "localhost") MQTT host to listen to
    -v,--verbose (0..7 default 4) Logging level, higher == more
]]

local cfg = {
    APP_NAME = "BLAH",
    topics = {
        "status/#",
        "command/#"
    },
    TOPIC_PUBLISH = "data/%s/senml",
}

local state = { should_run = true }
local mqtt

local function handle_mqtt_status(tag, rc, errno, errstr)
    if errno then
        printf("Unexpected MQTT Status(%s):%d-> %s\n", tag, errno, errstr)
    end
end

local function handle_ON_MESSAGE(mid, topic, payload, qos, retain)
    printf("ON_MESSAGE: topic:%s\n", topic)
    if payload == "quit" then
        printf("magic quit payload for valgrind!")
        state.should_run = false
    end
end

local function handle_ON_LOG(level, string)
    printf("mq log: %d->%s\n", level, string)
end

local function handle_ON_CONNECT(success, rc, str)
    print("on connect ->", success, rc, str)
    if not success then
        printf("Failed to connect: %d : %s\n", rc, str)
        return
    end
    state.mqtt_connected = true
    for _,v in pairs(cfg.topics) do
        if not mqtt:subscribe(v, 0) then
            -- An abort here is probably wildly overagressive.  could just call cleanup_mqtt() and let it try again.
            -- kinda hard to test this? maybe insert a delay in this handler? and stop broker in the meantime?
            printf("Aborting, unable to subscribe to MQTT: %s\n", v)
            os.exit(1)
        end
    end
end


local function main()
    mqtt = mosq.new()
    mqtt.ON_MESSAGE = handle_ON_MESSAGE
    mqtt.ON_CONNECT = handle_ON_CONNECT
    mqtt.ON_LOG = handle_ON_LOG
    mqtt:connect(args.host, 1883, 60)
    mqtt:loop_start()
    while state.should_run do
        -- tick tock...
        os.execute("sleep 1")
        printf("tick tock...\n")
    end
    mqtt:disconnect()
    mqtt:loop_stop()
end

main()
