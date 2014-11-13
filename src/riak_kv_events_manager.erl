%% -------------------------------------------------------------------
%%
%% riak_kv_event_manager: KV events
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
-module(riak_kv_events_manager).
-export([start_link/0]).
-export([post_hook/1]).

start_link() ->
    Result = {ok,_Pid} = gen_event:start_link({local, ?MODULE}),
    %% TODO: hard-code a handler here for now, fix later
    ok = gen_event:add_sup_handler(?MODULE, riak_kv_tcp_publisher, []),
    Result.

%% TODO: post-commit hook lives here for now, move it elsewhere later
post_hook(Obj) ->
    gen_event:notify(?MODULE, Obj).
