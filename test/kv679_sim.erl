%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 19 Aug 2014 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(kv679_sim).

-behaviour(gen_server).

-complile([export_all]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([sibling_explosion/1, vnode_data_loss/1, ec/1]).
-export([start_link/1, write/1, write/2, read/0, stop/0, drop_data/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {type, type_state}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Type) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Type], []).

%% @doc test that very basic EC properties of a coord/replicate are
%% met
ec(Mod) ->
    _ = start_link(Mod),
    write(a),
    write(b),
    write(c),
    {Ctx, Vals} = read(),
    ?assertEqual([a,b,c], lists:sort(Vals)),
    write(Ctx, d),
    {_Ctx2, Vals2} = read(),
    ?assertEqual([d], Vals2),
    write(Ctx, e),
    {_, Vals3} = read(),
    stop(),
    ?assertEqual([d,e], lists:sort(Vals3)).

sibling_explosion(Mod) ->
    _ = start_link(Mod),
    write(a),
    {CtxA, A} = read(),
    ?assertEqual([a], A),
    write(b),
    {CtxB, B} = read(),
    ?assertEqual([a, b], lists:sort(B)),
    %% Ctx writes
    write(CtxA, c),
    write(CtxB, d),
    {_Ctx, Values} = read(),
    stop(),
    %% if values contains a and b then siblings "exploded".
    %% The ctx write at CtxA should've removed a,
    %% The ctx write at CtxB should've remove a and b
    ?assertEqual([c, d], lists:sort(Values)).

%% @doc the case when a vnode is unable to read a value for a key it
%% _may_ have previously written. Given that some riak backends
%% (looking at you eleveldb) reoprt not found when reading a corrupted
%% block, and that the vnode logic itself treats a read error as not
%% found, and that sometimes no corruption is needed (operator wipes
%% the bitcask directory for some reason, "it's ok, there are N
%% replicas") Maybe this counts as "bizantine" or maybe not, I dunnno,
%% but it's a real data loss scenario.
vnode_data_loss(Mod) ->
    _ = start_link(Mod),
    %% Write something
    write(a),
    {Ctx, _} = read(),
    %% Write again using the given context
    write(Ctx, b),
    %% tell the coordinaor to "forget" what it knows (only the
    %% coordinator)
    drop_data(),
    %% Issue a new write, this write should be a sibling of whatever
    %% the current value is, as it has no context. If this value is
    %% not present on read, we lost data.
    write(c),
    {_, Values} = read(),
    stop(),
    ?assertEqual([b,c], lists:sort(Values)).

%% @doc doomstone. In this scenario, a tombstone that has not been
%% reaped (why?) can be handed off or read repaired and clobber new
%% values. In this case, it seems we should _know_ that the new values
%% are _not_ concurrent with, but dominate the tombstone. To pass this
%% scenario the new value must survive, and the tombstone be dropped.
doomstone(Mod) ->
    _ = start_link(Mod),
    %% write something
    write(a),
    {Ctx, _} = read(),
    Ctx = delete(Ctx),
    partial_reap(Ctx),
    write(b),
    {_, Val} = read(),
    ?assertEqual([b], Val).

write(Value) -> gen_server:call(?MODULE, {write, Value}).

write(Context, Value) ->
    gen_server:call(?MODULE, {write, Context, Value}).

read() ->
    gen_server:call(?MODULE, read).

%% @doc the coordinator will drop it's key/value data
drop_data() ->
    gen_server:call(?MODULE, drop).

stop() ->
    gen_server:call(?MODULE, stop).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Type]) ->
    TypeState = Type:init(),
    {ok, #state{type=Type, type_state=TypeState}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({write, Value}, _From, State) ->
    #state{type=Type, type_state=TypeState} = State,
    TypeState2 = Type:coordinate(Value, TypeState),
    {reply, ok, State#state{type_state=TypeState2}};
handle_call({write, Ctx, Value}, _From, State) ->
    #state{type=Type, type_state=TypeState} = State,
    TypeState2 = Type:coordinate(Value, Ctx, TypeState),
    {reply, ok, State#state{type_state=TypeState2}};
handle_call(read, _From, State) ->
    #state{type=Type, type_state=TypeState} = State,
    {{Ctx, Vals}, TypeState2} = Type:read(TypeState),
    {reply, {Ctx, Vals}, State#state{type_state=TypeState2}};
handle_call(drop, _From, State) ->
    #state{type=Type, type_state=TypeState} = State,
    TypeState2 = Type:drop_data(TypeState),
    {reply, ok, State#state{type_state=TypeState2}};
handle_call(stop, _From, State) ->
    {stop, normal, stopped, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
