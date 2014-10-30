%% -------------------------------------------------------------------
%%
%% riak_kv_notifier: notify coprocessors of kv events
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

-module(riak_kv_notifier).
-behaviour(gen_server).

-export([start_link/0, stop/0, notify/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          url,
          sock
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

notify(Term) ->
    gen_server:call(?MODULE, {notify, Term}).

init([]) ->
    process_flag(trap_exit, true),
    %% TODO: currently no app env is set for this address in config anywhere
    %% also, it's unclear whether this should be inproc, ipc, tcp localhost?
    %% and is there only one such endpoint, maybe several are needed for
    %% different types of communications?
    Url = app_helper:get_env(riak_kv, notifier_url, "inproc://riak_kv"),
    {ok, #state{url=Url}}.

handle_call({notify, Term}, _From, #state{url=Url, sock=undefined}=State) ->
    %% TODO: currently using push/pull sockets, probably should be pub/sub
    case enm:pub([{bind, Url}]) of
        {ok, Sock} ->
            ok = do_notify(Sock, Term),
            {reply, ok, State#state{sock=Sock}};
        Error ->
            {reply, Error, State}
    end;
handle_call({notify,Term}, _From, #state{sock=Sock}=State) ->
    {reply, do_notify(Sock, Term), State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sock=undefined}) ->
    ok;
terminate(_Reason, #state{sock=Sock}) ->
    enm:close(Sock),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_notify(Sock, Term) ->
    Payload = term_to_binary(Term),
    ok = enm:send(Sock, [<<"riak_kv_vnode">>, Payload]).
