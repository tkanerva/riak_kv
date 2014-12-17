-module(riak_kv_stat_mon).

-export([spawn_link/1, monitor_loop/1]).


spawn_link(Type) ->
    spawn_link(?MODULE, monitor_loop, [Type]).

monitor_loop(Type) ->
    receive
        {add_pid, Pid} ->
            erlang:monitor(process, Pid);
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            riak_kv_stat:perform_update({fsm_destroy, Type})
    end,
    monitor_loop(Type).
