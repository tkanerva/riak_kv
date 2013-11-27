-module(riak_kv_hooks).
-compile(export_all).

%% @doc
%% Called by {@link riak_kv_sup} to create the public ETS table used to
%% track registered hooks. Having `riak_kv_sup' own the table ensures
%% that the table exists aslong as riak_kv is running.
-spec create_table() -> ok.
create_table() ->
    ets:new(?MODULE, [named_table, public, bag,
                      {write_concurrency, true}, {read_concurrency, true}]),
    restore_state(),
    ok.

add_conditional_postcommit(Hook) ->
    add_hook(conditional_postcommit, Hook).

del_conditional_postcommit(Hook) ->
    del_hook(conditional_postcommit, Hook).

%% For now, only support typed buckets
get_conditional_postcommit({{BucketType, Bucket}, _Key}, BucketProps) ->
    Hooks = get_hooks(conditional_postcommit),
    ActiveHooks =
        [ActualHook || {Mod, Fun} <- Hooks,
                       ActualHook <- [Mod:Fun(BucketType, Bucket, BucketProps)],
                       ActualHook =/= false],
    lists:flatten(ActiveHooks);
get_conditional_postcommit(_BKey, _BucketProps) ->
    [].

%%%===================================================================

add_hook(Type, Hook) ->
    ets:insert(?MODULE, {Type, Hook}),
    save_state(),
    ok.

del_hook(Type, Hook) ->
    ets:delete_object(?MODULE, {Type, Hook}),
    save_state(),
    ok.

get_hooks(Type) ->
    [Hook || {_, Hook} <- ets:lookup(?MODULE, Type)].

%% Backup the current ETS state to the application environment just in case
%% riak_kv_sup dies and the ETS table is lost.
save_state() ->
    Hooks = ets:tab2list(?MODULE),
    ok = application:set_env(riak_kv, riak_kv_hooks, Hooks, infinity),
    ok.

%% Restore registered hooks in the unlikely case that riak_kv_sup died and
%% the ETS table was lost/recreated.
restore_state() ->
    case application:get_env(riak_kv, riak_kv_hooks) of
        undefined ->
            ok;
        {ok, Hooks} ->
            true = ets:insert_new(?MODULE, Hooks),
            ok
    end.
