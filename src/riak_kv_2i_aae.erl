%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Code related to repairing 2i data.
-module(riak_kv_2i_aae).
-behaviour(gen_fsm).

-include("riak_kv_wm_raw.hrl").

-export([start/2]).

-export([next_partition/2, wait_for_aae_pid/2, wait_for_repair/3]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%% How many items to scan before possibly pausing to obey speed throttle
-define(DEFAULT_SCAN_BATCH, 1000).
-define(MAX_DUTY_CYCLE, 100).
-define(MIN_DUTY_CYCLE, 1).
-define(LOCK_RETRIES, 10).
-define(AAE_PID_TIMEOUT, 30000).

-record(state,
        {
         speed,
         partitions,
         remaining,
         results = [],
         caller,
         reply_to,
         early_reply,
         aae_pid_req_id,
         worker_monitor
        }).

%% @doc Starts a process that will issue a repair for each partition in the
%% list, sequentially.
%% The Speed parameter controls how fast we go. 100 means full speed,
%% 50 means to do a unit of work and pause for the duration of that unit
%% to achieve a 50% duty cycle, etc.
%% If a single partition is requested, this function will block until that
%% partition has been blocked or that fails to notify the user immediately.
start(Partitions, Speed)
  when Speed >= ?MIN_DUTY_CYCLE, Speed =< ?MAX_DUTY_CYCLE ->
    Res = gen_fsm:start({local, ?MODULE}, ?MODULE,
                        [Partitions, Speed, self()], []),
    case {Res, length(Partitions)} of
        {{ok, _Pid}, 1} ->
            EarlyAck = gen_fsm:sync_send_all_state_event(?MODULE, early_ack,
                                                         infinity),
            case EarlyAck of
                lock_acquired ->
                    Res;
                _ ->
                    EarlyAck
            end;
        _ ->
            Res
    end.

init([Partitions, Speed, Caller]) ->
    lager:info("Starting 2i repair at speed ~p for partitions ~p",
               [Speed, Partitions]),
    {ok, next_partition, #state{partitions=Partitions, remaining=Partitions,
                                speed=Speed, caller=Caller}, 0}.

%% @doc Notifies caller for certain cases involving a single partition
%% so the user sees an error condition from the command line.
maybe_notify(State=#state{reply_to=undefined}, Reply) ->
    State#state{early_reply=Reply};
maybe_notify(State=#state{reply_to=From}, Reply) ->
    gen_fsm:reply(From, Reply),
    State#state{reply_to=undefined}.


%% @doc Process next partition or finish if no more remaining.
next_partition(timeout, State=#state{partitions=Partitions, remaining=[],
                                     results=Results}) ->
    %% We are done. Report results
    Errors = [Err || Err={_,{error,_}} <- Results],
    ErrMsg = case Errors of
                 [] -> "";
                 _ -> io_lib:format(" with errors ~p", [Errors])
             end,
    lager:info("Finished 2i repair for partitions ~p~s", [Partitions, ErrMsg]),
    {stop, normal, State};
next_partition(timeout, State=#state{remaining=[Partition|_]}) ->
    ReqId = make_ref(),
    riak_core_vnode_master:command({Partition, node()},
                                   {hashtree_pid, node()},
                                   {fsm, ReqId, self()},
                                   riak_kv_vnode_master),
    {next_state, wait_for_aae_pid, State#state{aae_pid_req_id=ReqId}, ?AAE_PID_TIMEOUT}.

%% @doc Waiting for vnode to send back the Pid of the AAE tree process.
wait_for_aae_pid(timeout, State) ->
    State2 = maybe_notify(State, {error, no_aae_pid}),
    {stop, no_aae_pid, State2};
wait_for_aae_pid({ReqId, {error, Err}}, State=#state{aae_pid_req_id=ReqId}) ->
    State2 = maybe_notify(State, {error, {no_aae_pid, Err}}),
    {stop, no_aae_pid, State2};
wait_for_aae_pid({ReqId, {ok, TreePid}},
                 State=#state{aae_pid_req_id=ReqId, speed=Speed,
                              remaining=[Partition|_]}) ->
    WorkerFun =
    fun() ->
            Res =
            repair_partition(Partition, Speed, ?MODULE, TreePid),
            gen_fsm:sync_send_event(?MODULE, {repair_result, Res})
    end,
    Mon = monitor(process, spawn(WorkerFun)),
    {next_state, wait_for_repair, State#state{worker_monitor=Mon}}.

%% @doc Waiting for a partition repair process to finish
wait_for_repair({lock_acquired, Partition},
                _From, 
                State=#state{remaining=[Partition|_]}) ->
    State2 = maybe_notify(State, lock_acquired),
    {reply, ok, wait_for_repair, State2};
wait_for_repair({repair_result, Res},
                _From,
                State=#state{remaining=[Partition|Rem],
                             results=Results,
                             worker_monitor=Mon}) ->
    State2 =
    case Res of
        {error, {lock_failed, LockErr}} ->
            maybe_notify(State, {lock_failed, LockErr});
        _ ->
            State
    end,
    demonitor(Mon, [flush]),
    {reply, ok, next_partition,
     State2#state{remaining=Rem,
                  results=[{Partition, Res}|Results],
                  worker_monitor=undefined},
     0}.

%% @doc Performs the actual repair work, called from a spawned process.
repair_partition(Partition, DutyCycle, From, TreePid) ->
    case get_hashtree_lock(TreePid, ?LOCK_RETRIES) of 
        ok ->
            lager:info("Acquired lock on partition ~p", [Partition]),
            gen_fsm:sync_send_event(From, {lock_acquired, Partition}),
            lager:info("Repairing indexes in partition ~p", [Partition]),
            case create_index_data_db(Partition, DutyCycle) of
                {ok, {DBDir, DBRef}} ->
                    Res =
                    case build_tmp_tree(Partition, DBRef, DutyCycle) of
                        {ok, TmpTree} ->
                            Res0 = do_exchange(Partition, DBRef, TmpTree,
                                               TreePid),
                            remove_tmp_tree(Partition, TmpTree),
                            Res0;
                        BuildTreeErr ->
                            BuildTreeErr
                    end,
                    destroy_index_data_db(DBDir, DBRef),
                    lager:info("Finished repairing indexes in partition ~p",
                               [Partition]),
                    Res;
                CreateDBErr ->
                    CreateDBErr
            end;
        LockErr ->
            lager:error("Failed to acquire hashtree lock on partition ~p",
                        [Partition]),
            {error, {lock_failed, LockErr}}
    end.

%% No wait for speed 100, wait equal to work time when speed is 50,
%% wait equal to 99 times the work time when speed is 1.
wait_factor(DutyCycle) ->
    if
        %% Avoid potential floating point funkiness for full speed.
        DutyCycle >= ?MAX_DUTY_CYCLE -> 0;
        true -> (100 - DutyCycle) / DutyCycle
    end.

scan_batch() ->
    app_helper:get_env(riak_kv, aae_2i_batch_size, ?DEFAULT_SCAN_BATCH).

%% @doc Create temporary DB holding 2i index data from disk.
create_index_data_db(Partition, DutyCycle) ->
    DBDir = filename:join(data_root(), "tmp_db"),
    (catch eleveldb:destroy(DBDir, [])),
    case filelib:ensure_dir(DBDir) of
        ok ->
            create_index_data_db(Partition, DutyCycle, DBDir);
        _ ->
            {error, io_lib:format("Could not create directory ~s", [DBDir])}
    end.

create_index_data_db(Partition, DutyCycle, DBDir) ->
    lager:info("Creating temporary database of 2i data in ~s", [DBDir]),
    DBOpts = leveldb_opts(),
    case eleveldb:open(DBDir, DBOpts) of
        {ok, DBRef} ->
            create_index_data_db(Partition, DutyCycle, DBDir, DBRef);
        {error, DBErr} ->
            {error, DBErr}
    end.

create_index_data_db(Partition, DutyCycle, DBDir, DBRef) ->
    Client = self(),
    BatchRef = make_ref(),
    WaitFactor = wait_factor(DutyCycle),
    ScanBatch = scan_batch(),
    Fun =
    fun(Bucket, Key, Field, Term, Count) ->
            BKey = term_to_binary({Bucket, Key}),
            OldVal = fetch_index_data(BKey, DBRef),
            Val = [{Field, Term}|OldVal],
            ok = eleveldb:put(DBRef, BKey, term_to_binary(Val), []),
            Count2 = Count + 1,
            case Count2 rem ScanBatch of
                0 when DutyCycle < ?MAX_DUTY_CYCLE ->
                    Mon = monitor(process, Client),
                    Client ! {BatchRef, self()},
                    receive
                        {BatchRef, continue} ->
                            ok;
                        {'DOWN', Mon, _, _, _} ->
                            throw(fold_receiver_died)
                    end,
                    demonitor(Mon, [flush]);
                _ ->
                    ok
            end,
            Count2
    end,
    lager:info("Grabbing all index data for partition ~p", [Partition]),
    Ref = make_ref(),
    Sender = {raw, Ref, Client},
    StartTime = now(),
    riak_core_vnode_master:command({Partition, node()},
                                   {fold_indexes, Fun, 0},
                                   Sender,
                                   riak_kv_vnode_master),
    NumFound = wait_for_index_scan(Ref, BatchRef, StartTime, WaitFactor),
    case NumFound of
        {error, _} = Err ->
            destroy_index_data_db(DBDir, DBRef),
            Err;
        _ ->
            lager:info("Grabbed ~p index data entries from partition ~p",
                       [NumFound, Partition]),
            {ok, {DBDir, DBRef}}
    end.

leveldb_opts() ->
    ConfigOpts = app_helper:get_env(riak_kv,
                                    anti_entropy_leveldb_opts,
                                    [{write_buffer_size, 20 * 1024 * 1024},
                                     {max_open_files, 20}]),
    Config0 = orddict:from_list(ConfigOpts),
    ForcedOpts = [{create_if_missing, true}, {error_if_exists, true}],
    Config =
    lists:foldl(fun({K,V}, Cfg) ->
                        orddict:store(K, V, Cfg)
                end, Config0, ForcedOpts),
    Config.

duty_cycle_pause(WaitFactor, StartTime) ->
    case WaitFactor > 0 of
        true ->
            Now = now(),
            ElapsedMicros = timer:now_diff(Now, StartTime),
            WaitMicros = ElapsedMicros * WaitFactor,
            WaitMillis = trunc(WaitMicros / 1000 + 0.5),
            timer:sleep(WaitMillis);
        false ->
            ok
    end.

wait_for_index_scan(Ref, BatchRef, StartTime, WaitFactor) ->
    receive
        {BatchRef, Pid} ->
            duty_cycle_pause(WaitFactor, StartTime),
            Pid ! {BatchRef, continue},
            wait_for_index_scan(Ref, BatchRef, now(), WaitFactor);
        {Ref, Result} ->
            Result
    end.

fetch_index_data(BK, DBRef) ->
    case eleveldb:get(DBRef, BK, []) of
        {ok, BinVal} ->
            binary_to_term(BinVal);
        _ ->
            []
    end.

%% @doc Remove all traces of the temporary 2i index DB
destroy_index_data_db(DBDir, DBRef) ->
    catch eleveldb:close(DBRef),
    eleveldb:destroy(DBDir, []).

%% @doc Returns the base data directory to use inside the AAE directory.
data_root() ->
    BaseDir = riak_kv_index_hashtree:determine_data_root(),
    filename:join(BaseDir, "2i").

%% @doc Builds a temporary hashtree based on the 2i data fetched from the
%% backend.
build_tmp_tree(Index, DBRef, DutyCycle) ->
    lager:info("Building tree for 2i data on disk for partition ~p", [Index]),
    %% Build temporary hashtree with this index data.
    TreeId = <<0:176/integer>>,
    Path = filename:join(data_root(), integer_to_list(Index)),
    catch eleveldb:destroy(Path, []),
    case filelib:ensure_dir(Path) of
        ok ->
            Tree = hashtree:new({Index, TreeId}, [{segment_path, Path}]),
            WaitFactor = wait_factor(DutyCycle),
            BatchSize = scan_batch(),
            FoldFun =
            fun({K, V}, {Count, TreeAcc, StartTime}) ->
                    Indexes = binary_to_term(V),
                    Hash = riak_kv_index_hashtree:hash_index_data(Indexes),
                    Tree2 = hashtree:insert(K, Hash, TreeAcc),
                    Count2 = Count + 1,
                    StartTime2 =
                    case Count2 rem BatchSize of
                        0 -> duty_cycle_pause(WaitFactor, StartTime),
                             now();
                        _ -> StartTime
                    end,
                    {Count2, Tree2, StartTime2}
            end,
            {Count, Tree2, _} = eleveldb:fold(DBRef, FoldFun,
                                              {0, Tree, erlang:now()}, []),
            lager:info("Done building temporary tree for 2i data "
                       "with ~p entries",
                       [Count]),
            {ok, Tree2};
        _ ->
            {error, io_lib:format("Could not create directory ~s", [Path])}
    end.

%% @doc Remove all traces of the temporary hashtree for 2i index data.
remove_tmp_tree(Partition, Tree) ->
    lager:debug("Removing temporary AAE 2i tree for partition ~p", [Partition]),
    hashtree:close(Tree),
    hashtree:destroy(Tree).

%% @doc Run exchange between the temporary 2i tree and the vnode's 2i tree.
do_exchange(Partition, DBRef, TmpTree, TreePid) ->
    lager:info("Reconciling 2i data"),
    IndexN2i = riak_kv_index_hashtree:index_2i_n(),
    lager:debug("Updating the vnode tree"),
    ok = riak_kv_index_hashtree:update(IndexN2i, TreePid),
    lager:debug("Updating the temporary 2i AAE tree"),
    {_, TmpTree2} = hashtree:update_snapshot(TmpTree),
    TmpTree3 = hashtree:update_perform(TmpTree2),
    Remote =
    fun(get_bucket, {L, B}) ->
            R1 = riak_kv_index_hashtree:exchange_bucket(IndexN2i, L, B,
                                                        TreePid),
            R1;
       (key_hashes, Segment) ->
            R2 = riak_kv_index_hashtree:exchange_segment(IndexN2i, Segment,
                                                         TreePid),
            R2;
       (A, B) ->
            throw({riak_kv_2i_aae_internal_error, A, B})
    end,
    AccFun =
    fun(KeyDiff, Acc) ->
            lists:foldl(
              fun({_DiffReason, BKeyBin}, Count) ->
                      BK = binary_to_term(BKeyBin),
                      IdxData = fetch_index_data(BKeyBin, DBRef),
                      riak_kv_vnode:refresh_index_data(Partition, BK, IdxData),
                      Count + 1
              end,
              Acc, KeyDiff)
    end,
    NumDiff = hashtree:compare(TmpTree3, Remote, AccFun, 0),
    lager:info("Found ~p differences", [NumDiff]),
    ok.


get_hashtree_lock(TreePid, Retries) ->
    case riak_kv_index_hashtree:get_lock(TreePid, local_fsm) of
        Reply = already_locked ->
            case Retries > 0 of
                true ->
                    timer:sleep(1000),
                    get_hashtree_lock(TreePid, Retries-1);
                false ->
                    Reply
            end;
        Reply ->
            Reply
    end.


%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Handles repair worker death notification
handle_info({'DOWN', Mon, _, _, Reason}, wait_for_repair,
            State=#state{worker_monitor=Mon, remaining=[Partition|_]}) ->
    lager:error("Index repair of partition ~p failed : ~p",
                [Partition, Reason]),
    {stop, {partition_failed, Reason}, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(early_ack, From, StateName,
                  State=#state{early_reply=undefined}) ->
    {next_state, StateName, State#state{reply_to=From}};
handle_sync_event(early_ack, _From, StateName,
                  State=#state{early_reply=Reply}) ->
    {reply, Reply, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
