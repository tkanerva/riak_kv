%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%% dvvset strategy for kv679 test
%%% @end
%%% Created : 19 Aug 2014 by Russell Brown <russelldb@basho.com>

-module(kv679_dvvset).

-behaviour(kv679).

-export([init/0, coordinate/2, coordinate/3, read/1, drop_data/1]).

-record(state, {coord,
                rep_a,
                rep_b}).

init() ->
    #state{}.

%% Behaviour API
coordinate(Value, State) ->
    #state{coord=Coord0, rep_a=A0, rep_b=B0} = State,
    Coord = coord_merge(dvvset:new(Value), Coord0),
    {A, B} = replicate(A0, B0, Coord),
    State#state{coord=Coord, rep_a=A, rep_b=B}.

coordinate(Value, Ctx, State) ->
    #state{coord=Coord0, rep_a=A0, rep_b=B0} = State,
    Coord = coord_merge(dvvset:new(Ctx, Value), Coord0),
    {A, B} = replicate(A0, B0, Coord),
    State#state{coord=Coord, rep_a=A, rep_b=B}.

drop_data(State) ->
    State#state{coord=undefined}.

read(State) ->
    #state{coord=Coord, rep_a=A, rep_b=B} = State,
    DVV = dvvset:sync([Coord, A, B]),
    Vals = dvvset:values(DVV),
    Ctx = dvvset:join(DVV),
    {{Ctx, Vals}, State#state{coord=DVV, rep_a=DVV, rep_b=DVV}}.

%% Local private
coord_merge(DVV, undefined) ->
    %% No local copy of key
    dvvset:update(DVV, coord);
coord_merge(PutDVV, LocalDVV) ->
    dvvset:update(PutDVV, LocalDVV, coord).

replicate(A0, B0, O) ->
    A = merge(A0, O),
    B = merge(B0, O),
    {A, B}.

merge(undefined, undefined) ->
    {[], []};
merge(undefined, B) ->
    B;
merge(A, undefined) ->
    A;
merge(DVV1, DVV2) ->
    dvvset:sync([DVV1, DVV2]).
