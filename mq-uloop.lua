#!/usr/bin/lua
--[[
Karl Palsson, 2016 <karlp@tweak.net.au>
Demo of event based reconnecting mqtt client using OpenWrt's uloop/libubox
Not entirely happy with how verbose it is, with flags, but uses only 
the async methods, and successfully reconnects well.
]]

local bit = require("bit")
local uloop = require("uloop")
uloop.init()
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
    MOSQ_IDLE_LOOP_MS = 250,
    topics = {
        "status/#",
        "command/#"
    },
    TOPIC_PUBLISH = "data/%s/senml",
}

local state = { mqtt_reconnection_count = 0 }
local mqtt

local function cleanup_mqtt()
    state.mqtt_fdr:delete()
    state.mqtt_fdw:delete()
    state.mqtt_idle_timer:cancel()
end

local function handle_mqtt_status(tag, rc, errno, errstr)
    if errno then
        printf("Unexpected MQTT Status(%s):%d-> %s\n", tag, errno, errstr)
        cleanup_mqtt()
    end
end

local function handle_ON_MESSAGE(mid, topic, payload, qos, retain)
    printf("ON_MESSAGE: topic:%s\n", topic)
    if payload == "quit" then
        printf("magic quit payload for valgrind!")
        uloop.cancel()
    end
end

local function handle_ON_LOG(level, string)
    printf("mq log: %d->%s\n", level, string)
end

local function handle_ON_DISCONNECT(was_clean, rc, str)
    if was_clean then
        printf("Client explicitly requested disconnect\n")
    else
        printf("Unexpected disconnection: %d -> %s\n", rc, str)
    end
    state.mqtt_connected = false
end

local function handle_ON_CONNECT(success, rc, str)
    state.mqtt_working = false
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

--- Socket event handers
-- @param ufd unused, but would be the mosquitto socket
-- @param events read/write
--
local function mqtt_fd_handler(ufd, events)
    if bit.band(events, uloop.ULOOP_READ) == uloop.ULOOP_READ then
        handle_mqtt_status("read", mqtt:loop_read())
    elseif bit.band(events, uloop.ULOOP_WRITE) == uloop.ULOOP_WRITE then
        handle_mqtt_status("write", mqtt:loop_write())
    end
    -- We don't recurse here so we actually share cpu
    if mqtt:want_write() then
        uloop.timer(function()
            -- fake it til you make it!
            mqtt_fd_handler(ufd, uloop.ULOOP_WRITE)
        end, 10) -- "now"
    end
end

--- Periodic task for maintaining connections and retries within client
--
local function mqtt_idle_handler()
    handle_mqtt_status("misc", mqtt:loop_misc())
    state.mqtt_idle_timer:set(cfg.MOSQ_IDLE_LOOP_MS)
end

--- Initiate a connection, and install event handlers
--
local function setup_mqtt()
    mqtt.ON_MESSAGE = handle_ON_MESSAGE
    mqtt.ON_CONNECT = handle_ON_CONNECT
    mqtt.ON_LOG = handle_ON_LOG
    mqtt.ON_DISCONNECT = handle_ON_DISCONNECT
    local _, errno, strerr = mqtt:connect_async(args.host, 1883, 60)
    if errno then
        -- Treat this as "fatal", means internal syscalls failed.
        error("Failed to connect: " .. strerr)
    end
    state.mqtt_working = true

    state.mqtt_fdr = uloop.fd_add(mqtt:socket(), mqtt_fd_handler, uloop.ULOOP_READ)
    state.mqtt_fdw = uloop.fd_add(mqtt:socket(), mqtt_fd_handler, uloop.ULOOP_WRITE)
    state.mqtt_idle_timer = uloop.timer(mqtt_idle_handler, cfg.MOSQ_IDLE_LOOP_MS)
    if mqtt:want_write() then
        uloop.timer(function()
            mqtt_fd_handler(mqtt:socket(), uloop.ULOOP_WRITE)
        end, 10) -- "now"
    end

end

--- Backoff on reconnection attempts a little bit
-- (totally unnecessary for this demo)
local function mqtt_calculate_reconnect(st)
    if st.mqtt_reconnection_count < 5 then
        return 1500
    end
    if st.mqtt_reconnection_count < 20 then
        return 2500
    end
    return 5000
end


local function main()
    mqtt = mosq.new()
    setup_mqtt()

    -- test the mqtt connection and trigger a reconnection if needed.
    state.mqtt_stay_alive = uloop.timer(function()
        state.mqtt_stay_alive:set(mqtt_calculate_reconnect(state))
        if not state.mqtt_connected then
            if not state.mqtt_working then
                uloop.timer(function()
                    if not pcall(setup_mqtt) then
                        state.mqtt_reconnection_count = state.mqtt_reconnection_count + 1
                        printf("Failed mqtt reconnection attempt: %d\n", state.mqtt_reconnection_count)
                    else
                        state.mqtt_reconnection_count = 0
                    end
                end, 50) -- "now"
            end
        end
    end, mqtt_calculate_reconnect(state))

    uloop.run()
    printf(">>>post uloop.run()\n")
    cleanup_mqtt()
end

main()
