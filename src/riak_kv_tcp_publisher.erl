%% -------------------------------------------------------------------
%%
%% riak_kv_tcp_publisher: publish KV events on a TCP port
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_kv_tcp_publisher).
-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

%% internal export
-export([socket_handler/1]).

-record(state, {
          sockpid
         }).

init([]) ->
    [{_, Port}] = app_helper:get_env(riak_api, pbevents, [{"0.0.0.0", 8092}]),
    SockPid = spawn_link(?MODULE, socket_handler, [Port]),
    {ok, #state{sockpid=SockPid}}.

handle_event(Obj, #state{sockpid=SockPid}=State) ->
    true = riak_object:is_robject(Obj),
    Content = riak_object:get_contents(Obj),
    Vc = riak_object:vclock(Obj),
    Bucket = riak_object:bucket(Obj),
    Msg0 = #rpbputresp{content=riak_pb_kv_codec:encode_contents(Content),
                       vclock=riak_object:encode_vclock(Vc),
                       key=riak_object:key(Obj)},
    Msg = case Bucket of
              {Type,Bkt} ->
                  Msg0#rpbputresp{bucket=Bkt,type=Type};
              _ ->
                  Msg0#rpbputresp{bucket=Bucket}
          end,
    {ok, EncMsg} = riak_kv_pb_object:encode(Msg),
    SockPid ! {send, EncMsg},
    {ok, State}.

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, #state{sockpid=SockPid}) ->
    SockPid ! stop,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% TODO: an unsupervised socket handler process, needs to be coded better
socket_handler(Port) ->
    ListenOpts = [binary,{active,false},{reuseaddr,true}],
    {ok, LS} = gen_tcp:listen(Port, ListenOpts),
    {ok, ARef} = prim_inet:async_accept(LS, -1),
    socket_handler(LS, ARef, []).
socket_handler(LS, ARef, Subs) ->
    receive
        {inet_async,LS,ARef,{ok,Sub}} ->
            inet_db:register_socket(Sub, inet_tcp),
            inet:setopts(Sub, [{active,once}]),
            {ok, NewARef} = prim_inet:async_accept(LS, -1),
            socket_handler(LS, NewARef, [Sub|Subs]);
        {send, Data} ->
            [gen_tcp:send(S, Data) || S <- Subs],
            socket_handler(LS, ARef, Subs);
        stop ->
            lists:foreach(fun(S) -> gen_tcp:close(S) end, Subs),
            gen_tcp:close(LS);
        {tcp_closed,Sub} ->
            NewSubs = lists:delete(Sub,Subs),
            socket_handler(LS, ARef, NewSubs)
    end.
