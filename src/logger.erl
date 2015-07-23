-module(logger).
-export([logging_is_on/0,log/1,
	 init/1,handle_cast/2,terminate/2,
	 correct_content_test/0,
	 correct_no_files_test/0,
	 duration_works_test/0]).


%TODO ask whether use of now() is suitable
%TODO put records in suitable header
-record(logger_state,{log_end_time=infinity, %in milliseconds
		      log=no_log,
		      is_logging=false}).
-record(oplog_on_request,{size,duration,salt}).

%1 GB
-define(DEFAULT_LOG_SIZE,(1024*1024*1024)).

%Non-genserver callbacks associated with the logger:
-spec logging_is_on() -> bool().
logging_is_on()->ets:lookup(logger_state_ets,is_logging)==[{is_logging,true}].
set_logging_is_on(Bool)->ets:insert(logger_state_ets,{is_logging,Bool}).
log(Term)->
	gen_server:cast(logger,{log,Term}).
end_current_log()->
	gen_server:cast(logger,#oplog_on_request{duration=0}).

%Each node has a log, so it doesn't need to be in distributed mode
%Is logger too generic a name for the module & process?
init(_) ->
	case whereis(logger) of
	     undefined -> 
	     	       register(logger,self()),
		       T = logger_state_ets,
		       [ets:new(T,[named_table]) || ets:info(T) == undefined],
		       set_logging_is_on(false);
	     _Pid  -> 
	       [end_current_log()
	       || whereis(logger) /= undefined] 
	       %resets state if logger exists
	end,
	{ok,#logger_state{}}.

%Opens a new log in response to oplog on <args>
%First call to oplog on opens new log, even w/o args
%Log name internal to genserver.
%Does not take salt as argument for now.
new_log(Size) ->
	Time = now(), %not too important exactly when it was opened?
	Node = node(),
	[disk_log:close(logger_log) || disk_log:info(logger_log) 
				       /= {error,no_such_log}],
	{ok,Log} = disk_log:open([
		{name,logger_log},
		{file,lists:flatten(io_lib:format("log/logger_log~p~p.log",
						[Node,Time]))},
		{type,halt},
		{format,internal},
		{mode,read_write},
		{size,Size}
		      ]),
	Log.
disk_log(Log,Term) -> ok = disk_log:log(Log,Term).

%logger will likely crash upon log filling up, will supervisor manage that?
handle_cast(oplog_on,State) ->
	set_logging_is_on(true),
	{noreply,State#logger_state{is_logging=true,
				    log=case State#logger_state.log of
	     			    	     no_log -> 
					     new_log(?DEFAULT_LOG_SIZE);
	     			    	     L -> L end}};
handle_cast(#oplog_on_request{size=Size,duration=Millis},State)->
	set_logging_is_on(true),
	case State#logger_state.log of
	     no_log -> ok;
	     L -> disk_log:close(L)
	end,
	{noreply,
	case Millis > 0 of
	     false -> 
	     	   disk_log:close(logger_log),
	     	   #logger_state{};
	     true -> 
	     	  #logger_state{
			log_end_time = time_in_millis() + Millis,
		  	log = new_log(Size),
		  	is_logging = true
	     	        }
	end};
handle_cast(dbg,State)->io:format("~p",[State]),{noreply,State};
handle_cast(oplog_off,State)->
	set_logging_is_on(false),
	{noreply,State#logger_state{is_logging=false}};
%Not using genserver would allow us to put timeout in receive clause,
%avoiding the comparison of time_in_millis to log_end_time.
handle_cast({log,Term},State = #logger_state{
					log_end_time=End,
					log=L,
					is_logging=IL})->
	{noreply,
	case IL of
	     false -> State;
	     true -> 
	     	     case time_in_millis() > End of
		     	  true -> disk_log:close(L),
			       	  #logger_state{};
	     	     	  false -> 
			  	disk_log(L,Term),
		     		State
		     end
	end}.


terminate(_Reason,#logger_state{log = L})->
	disk_log:close(L).

time_in_millis()-> {Megas,Seconds,Micros} = now(),
		   Megas*1000000000+Seconds*1000+round(Micros/1000).

start()->
	gen_server:start(logger,no_args,[]).
%Docs and my version of erlang disagree, maybe change on surface?
stop()->gen_server:system_terminate(no_reason,no_parent,no_debug,
			[logger,"No idea, shutdown came from outside",
			logger,no_time]).
correct_content_test()->
	start(),
	gen_server:cast(logger,oplog_on),
	log(alice),
	log(bob),
	gen_server:cast(logger,oplog_off),
	log(charles),
	timer:sleep(100),
	{{continuation,_,_,_},Log_Contents} = disk_log:chunk(logger_log,start),
	Log_Contents == [alice,bob].

correct_no_files_test()->
	start(),
	ForeachFile = fun(F) -> 
		      	    {ok,Files} = file:list_dir_all("log"),
			    lists:map(F,Files)
		      end,
	ForeachFile(fun(File)->ok = file:rename("log/"++File,
						"log/OLD"++File) end),
	gen_server:cast(logger,oplog_on),
	end_current_log(),
	gen_server:cast(logger,oplog_on),
	end_current_log(),
	gen_server:cast(logger,oplog_on),
	end_current_log(),
	timer:sleep(100),
	length(lists:filter(fun(X)->X end,
	ForeachFile(fun(File) -> 
			      case File of
			      	      "OLD" ++ File2 ->
				      	    file:rename("log/"++File,
							"log/"++File2),
					    false;
				      _ -> ok = file:delete("log/"++File),
				      	   true
			      end
		    end)))==3.
duration_works_test()->
	start(),
	gen_server:cast(logger,#oplog_on_request{size = ?DEFAULT_LOG_SIZE,
						 duration=1}),
	timer:sleep(1000),
	eof=disk_log:chunk(logger_log,start),
	log(value),%this should close log
	timer:sleep(100),
	{error,no_such_log} == disk_log:info(logger_log).
