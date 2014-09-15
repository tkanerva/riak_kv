%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%% behaviour for strategies for kv679 test
%%% @end
%%% Created : 19 Aug 2014 by Russell Brown <russelldb@basho.com>

-module(kv679).

-type state() :: any().
-type value() :: any().
-type ctx() :: vclock:vclock().


-callback init() ->
    state().

-callback coordinate(value(), state()) ->
    state().

-callback coordinate(value(), ctx(), state()) ->
    state().

-callback drop_data(state()) ->
    state().

%% -callback replicate(state()) ->
%%     state().

-callback read(state()) ->
    {{ctx(), [value()]}, state()}.



