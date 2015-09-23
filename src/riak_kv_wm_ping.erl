%% -------------------------------------------------------------------
%%
%% riak_kv_wm_ping: simple Webmachine resource for availability test
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc simple Webmachine resource for availability test

-module(riak_kv_wm_ping).

%% webmachine resource exports
-export([
         init/1,
         is_authorized/2,
         to_html/2
        ]).

-record(ctx, {api_version,  %% integer() - Determine which version of the API to use.
              bucket_type,  %% binary() - Bucket type (from uri)
              bucket,       %% binary() - Bucket name (from uri)
              client,       %% riak_client() - the store client
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              allow_props_param, %% true if the user can also list props. (legacy API)
              timeout,      %% integer() - list keys timeout
              security      %% security context
             }).

-include_lib("webmachine/include/webmachine.hrl").

init([]) ->
    {ok, undefined}.

is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            Context = #ctx{security=SecContext},
            {true, ReqData, Context};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

to_html(ReqData, undefined) ->
    riak_kv_wm_utils:log_http_access(success, ReqData, unknown),
    {"OK", ReqData, undefined};

to_html(ReqData, Ctx) ->
    riak_kv_wm_utils:log_http_access(success, ReqData, riak_core_security:get_username(Ctx#ctx.security)),
    {"OK", ReqData, Ctx}.
