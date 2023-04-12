%% Date: 28.12.16 - 18:17
%% Ⓒ 2016 heyoka
-module(df_component).
-author("Alexander Minichmair").

-include("faxe_common.hrl").

-behaviour(gen_server).

%% API
-export([start_link/6]).
-export([start_node/3, inports/1, outports/1, start_async/3, wants/1, emits/1, add_options/0, add_options/1]).

%% Callback API

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).


-define(MSG_Q_LENGTH_HIGH_WATERMARK, 15).

-define(MIN_IDLE_TIME, 30000).
-define(MIN_IDLE_CHECK_INTERVAL, 15000).

-type auto_request()    :: 'all' | 'emit' | 'none'.
-type item_type()       :: nil | batch | point | both.

-type df_port()         :: non_neg_integer().


%%%===================================================================
%%% CALLBACKS
%%%===================================================================

%% @doc
%% INIT/3
%%
%% initialisation
%%
-callback init(NodeId :: term(), Inputs :: list(), Args :: term())
       -> {ok, auto_request(), cbstate()}.



%% @doc
%% PROCESS/2
%%
%% process value or batch incoming on specific inport
%%
%% return values :
%%
%% :: just return the state
%% {ok, cbstate()}
%%
%% :: used to emit a value right after processing
%% {emit, Port :: port(), Value :: term(), cbstate()} :: used emit right after processing
%%
%% :: request a value in return of a process call
%% {request, ReqPort :: port(), ReqPid :: pid(), cbstate()}
%%
%% :: emit and request a value
%% {emit_request, OutPort :: port(), Value :: term(), ReqPort :: port(), ReqPid :: pid(), cbstate()}
%% @end
%%
-callback process(Inport :: non_neg_integer(), Value :: #data_point{} | #data_batch{}, State :: cbstate())
       ->
       {ok, cbstate()} |

       {emit,
          { Port :: df_port(), Value :: term() }, cbstate()
       } |
       {emit,
          Value :: term(), cbstate()
       } |
       {request,
          { ReqPort :: df_port(), ReqPid :: pid() }, cbstate()
       } |
       {emit_request,
          { OutPort :: df_port(), Value :: term() }, { ReqPort :: df_port(), ReqPid :: pid() }, cbstate()
       } |
       {emit_request,
          Value :: term(), { ReqPort :: df_port(), ReqPid :: pid() }, cbstate()
       } |
       {emit_ack,
          { OutPort :: df_port(), Value :: term() }, DTag :: non_neg_integer(), cbstate()
       } |
       {emit_ack,
          Value :: term(), DTag :: non_neg_integer(), cbstate()
       } |

       {error, Reason :: term()}.


%%%==========================================================
%%% OPTIONAL CALLBACKS
%%% =========================================================

%% @doc
%% OPTIONS/0
%%
%% optional
%%
%% retrieve options (with default values optionally) for a component
%% for an optional parameter, provide Default term
%%
%% options with no 'Default' value, will be treated as mandatory
%% @end
-callback options() ->
   list(
   {Name :: atom(), Type :: dataflow:option_value(), Default :: dataflow:option_value()|dataflow:option_config()} |
   {Name :: atom(), Type :: atom()}
   ).
%% @end

%% @doc
%% further checks for options, see dataflow module
%% optional
%% @end
-callback check_options() ->
   list(
   {Type :: atom(), term()} | {Type :: atom(), term(), term()} | {Type :: atom(), term(), term(), term()}
   ).

%% @doc
%% info about possible types of input and output data
%% optional
%% @end
-callback wants() -> item_type().
-callback emits() -> item_type().

%% @doc
%% INPORTS/0
%%
%% optional
%% provide a list of inports for the component :
%%
-callback inports()  -> {ok, list()}.



%% @doc
%% OUTPORTS/0
%%
%% optional
%% provide a list of outports for the component:
%%
-callback outports() -> {ok, list()}.



%% @doc
%% HANDLE_INFO/2
%%
%% optional
%% handle other messages that will be sent to this process :
%%
%% @end
-callback handle_info(Request :: term(), State :: cbstate()) ->
   {ok, NewCallbackState :: cbstate()} |
   {emit, {Port :: non_neg_integer(), Value :: term()}, NewCallbackState :: cbstate()} |
   {error, Reason :: term()}.

%% @doc
%% HANDLE_ACK/3
%%
%% optional
%% handle data acknowledge messages from downstream nodes :
%%
%% @end
-callback handle_ack(Mode :: single|multi, DTag :: non_neg_integer(), State :: cbstate()) ->
   {ok, NewCallbackState :: cbstate()} |
   {error, Reason :: term()}.


%% @doc
%% SHUTDOWN/1
%%
%% optional
%% called when component process is about to stop :
%%
%% @end
-callback shutdown(State :: cbstate())
       -> any().

%% these are optional (%% erlang 18+)
-optional_callbacks([
options/0, check_options/0, wants/0, emits/0,
inports/0, outports/0,
handle_info/2, handle_ack/3, shutdown/1]).



%%%===================================================================
%%% API
%%%===================================================================

add_options() ->
   add_options(<<"">>).

add_options(Default) ->
   [
      {'_name', string, Default},
      {'_stop_idle', boolean, false},
      {'_idle_time', duration, <<"5m">>}
   ].

-spec(start_link(atom(), term(), term(), list(), list(), term()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Component, GraphId, NodeId, Inports, Outports, Args) ->
   gen_server:start_link(?MODULE, [Component, GraphId, NodeId, Inports, Outports, Args], []).

start_node(Server, Inputs, FlowMode) ->
   gen_server:call(Server, {start, Inputs, FlowMode}).

start_async(Server, Inputs, FlowMode) ->
   Server ! {start, Inputs, FlowMode}.

inports(Module) ->
   case erlang:function_exported(Module, inports, 0) of
      true -> Module:inports();
      false -> inports()
   end.

outports(Module) ->
   case erlang:function_exported(Module, outports, 0) of
      true -> Module:outports();
      false -> outports()
   end.

-spec wants(atom()) -> item_type().
wants(Module) when is_atom(Module) ->
   case erlang:function_exported(Module, wants, 0) of
      true -> Module:wants();
      false -> both
   end.

-spec emits(atom()) -> item_type().
emits(Module) when is_atom(Module) ->
   case erlang:function_exported(Module, emits, 0) of
      true -> Module:emits();
      false -> both
   end.



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
   {ok, State :: #c_state{}} | {ok, State :: #c_state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([Component, GraphId, NodeId, Inports, _Outports, Args]) ->
%%   lager:warning("inports: ~p" ,[Inports]),
   code:ensure_loaded(Component),
   lager:md([{flow, GraphId}, {comp, NodeId}]),
   InputPorts = lists:map(fun({_Pid, Port}) -> Port end, Inports),
   NId = <<GraphId/binary, "==", NodeId/binary>>,
   {ok, #c_state{
      component = Component, graph_id = GraphId, node_id = NodeId, flow_node_id = NId,
      node_index = {GraphId, NodeId}, inports = InputPorts, cb_state = Args}
   };
init(#c_state{} = PersistedState) ->
   {ok, PersistedState}.

%% sync start
handle_call({start, Inputs, FlowMode}, _From,
    State=#c_state{component = CB, cb_state = CBState, node_index = NodeIndex}) ->

   lager:debug("component ~p starts with options; ~p", [CB, CBState]),
   Opts = CBState,

   %% idle_stop feature
   NewState = setup_idle_stop(State, Opts),

   Inited = CB:init(NodeIndex, Inputs, Opts),
   {AReq, NewCBState} =
      case Inited of

         {ok, ARequest, NCBState}      -> {ARequest, NCBState};
         {ok, NCBState}                -> {all, NCBState};
         {error_options, Message}      -> erlang:error(Message);
         {error, What}                 -> erlang:error(What)
      end,

   AR = case FlowMode of pull -> AReq; push -> none end,
   CallbackHandlesInfo = erlang:function_exported(CB, handle_info, 2),
   CallbackHandlesAck = erlang:function_exported(CB, handle_ack, 3),
   %% metrics
%%   folsom_metrics:new_histogram(NId, slide, 60),
%%   folsom_metrics:new_history(<< NId/binary, ?FOLSOM_ERROR_HISTORY >>, 24),
   {reply, ok,
      NewState#c_state{
         inports = Inputs,
         auto_request = AR,
         cb_state = NewCBState,
         cb_inited = true,
         flow_mode = FlowMode,
         cb_handle_info = CallbackHandlesInfo,
         cb_handle_ack = CallbackHandlesAck}
   };
handle_call(get_subscribers, _From, State=#c_state{node_index = NodeIndex}) ->
   {reply, {ok, df_subscription:subscriptions(NodeIndex)}, State}
;
handle_call(_What, _From, State) ->
   lager:info("~p: unexpected handle_call with ~p",[?MODULE, _What]),
   {reply, error, State}
.

handle_cast(_Request, State) ->
   {noreply, State}.


%% @doc
%% handle_info/2
%% these are the messages from and to other dataflow nodes
%% do not use these tags in your callback 'handle_info' functions :
%% 'start' | 'request' | 'item' | 'emit' | 'pull' | 'stop'
%% you will not receive the info message in the callback with these
%%


%% @doc
%% start the node asynchronously
%% @end
handle_info({start, Inputs, FlowMode},
    State=#c_state{component = CB, cb_state = CBState, node_index = NodeIndex}) ->

%%   lager:info("component ~p starts with options; ~p and inputs: ~p", [CB, CBState, Inputs]),
   Opts = CBState,

   %% stop flow on idle feature
   NewState = setup_idle_stop(State, Opts),

   Inited = CB:init(NodeIndex, Inputs, Opts),
   {AReq, NewCBState} =
      case Inited of

         {ok, ARequest, NCBState}      -> {ARequest, NCBState};
         {ok, NCBState}                -> {all, NCBState};
         {error_options, Message}      -> lager:error("Starting failed with Reason: ~p",[Message]), erlang:error(Message);
         {error, What}                 -> lager:error("Starting failed with Reason: ~p",[What]), erlang:error(What)
      end,

   AR = case FlowMode of pull -> AReq; push -> none end,
   CallbackHandlesInfo = erlang:function_exported(CB, handle_info, 2),
   CallbackHandlesAck = erlang:function_exported(CB, handle_ack, 3),

   {noreply,
      NewState#c_state{
         inports = Inputs,
         auto_request = AR,
         cb_state = NewCBState,
         cb_inited = true,
         flow_mode = FlowMode,
         cb_handle_info = CallbackHandlesInfo,
         cb_handle_ack = CallbackHandlesAck
      }
   }
;
handle_info(check_stop_on_idle, State = #c_state{idle_since = Since, idle_check_interval = Interval,
      idle_time = IdleFor}) ->
   IdleTime = (faxe_time:now()-Since),
%%   lager:notice("~p idle for ~p secs",[State#c_state.flow_node_id, round(IdleTime/1000)]),
   case IdleTime >= IdleFor of
      true ->
         %% I think we do not have to check for message queue length of the process, because, if there were
         %% messages still, we would see that in the handle_info callbacks
         %% (check for any buffer in a node also)
         %% maybe provide a callback (ie: is_idle()) for the module to make sure we really are idle, basically asking:
         %% "is there any hidden work still to be done ?"
         lager:notice("IDLE TIMEOUT !!!"),
         faxe:stop_task(State#c_state.graph_id, true);
      false ->
         erlang:send_after(Interval, self(), check_stop_on_idle)
   end,
   {noreply, State};


%%% DEBUG
handle_info(start_debug, State = #c_state{}) ->
   NewState = cb_handle_info(start_debug, State),
   {noreply, NewState#c_state{emit_debug = true}};
handle_info(stop_debug, State = #c_state{}) ->
   NewState = cb_handle_info(stop_debug, State),
   {noreply, NewState#c_state{emit_debug = false}};

handle_info({request, ReqPid, ReqPort}, State = #c_state{node_index = NodeIndex}) ->
   true = df_subscription:request(NodeIndex, ReqPid, ReqPort),
   {noreply, State};

handle_info({ack, Mode, DTag} = Req, State = #c_state{inports = Ins}) ->
   NewState = cb_handle_ack(Mode, DTag, State),
   lists:foreach(fun({_Port, Pid}) -> Pid ! Req end, Ins),
   {noreply, NewState#c_state{emit_debug = false}};

%% RECEIVING ITEM
handle_info({item, _}, State=#c_state{cb_inited = false}) ->
   %% drop it, we are not yet initialized
   lager:notice("Got Item but callback not yet initialized, item will be dropped!"),
   {noreply, reset_idle(State, item_no_init)};
handle_info({item, {Inport, Value}},
    State=#c_state{
       cb_state = CBState, component = Module, flow_mode = FMode, auto_request = AR}) ->

   metric(?METRIC_ITEMS_IN, 1, State),
   maybe_debug(item_in, Inport, Value, State),

%%   TStart = erlang:monotonic_time(microsecond),
%%   Result = (Module:process(Inport, Value, CBState)),
   case  catch(Module:process(Inport, Value, CBState)) of
      {'EXIT', {Reason, Stacktrace}} ->
         lager:error("'error' in component ~p caught when processing item: ~p -- ~p",
            [State#c_state.component, {Inport, Value}, lager:pr_stacktrace(Stacktrace, {'EXIT', Reason})]),
         metric(?METRIC_ERRORS, 1, State),
         {noreply, State};

      Result ->
         {NewState, Requested, REmitted} = handle_process_result(Result, State),
%%         metric(?METRIC_PROCESSING_TIME, (erlang:monotonic_time(microsecond)-TStart)/1000, State),
         case FMode == pull of
            true -> case {Requested, AR, REmitted} of
                       {true, _, _} -> ok;
                       {false, none, _} -> ok;
                       {false, emit, false} -> ok;
                       {false, emit, true} -> request_all(State#c_state.inports, FMode);
                       {false, all, _} -> request_all(State#c_state.inports, FMode)
                    end;
            false -> ok
         end,
         {noreply, reset_idle(NewState, item)}
   end
;
%% EMITTING ITEM
handle_info({emit, {Outport, Value}}, State=#c_state{node_id = _NId,
   flow_mode = FMode, auto_request = AR, emitted = EmitCount}) ->
   emit(Outport, Value, State),
   case AR of
      none  -> ok;
      _     -> request_all(State#c_state.inports, FMode)
   end,
   {noreply, reset_idle(State#c_state{emitted = EmitCount+1}, emit)};

handle_info(pull, State=#c_state{inports = Ins}) ->
   lists:foreach(fun({Port, Pid}) -> dataflow:request_items(Port, [Pid]) end, Ins),
   {noreply, State}
;
handle_info(stop, State=#c_state{node_id = _N, component = Mod, cb_state = CBState}) ->
   case erlang:function_exported(Mod, shutdown, 1) of
      true -> Mod:shutdown(CBState);
      false -> ok
   end,
   {stop, normal, State}
;

%% Callback Module handle_info
handle_info(Req, State = #c_state{component = Module, cb_state = CB, cb_handle_info = true}) ->
   case Module:handle_info(Req, CB) of
      {ok, CB0} ->
         {noreply, State#c_state{cb_state = CB0}};
      {emit, {_Port, _Val} = Data, CB1} ->
         handle_info({emit, Data}, State#c_state{cb_state = CB1});
      {emit, Data, CB1} ->
         handle_info({emit, {1, Data}}, State#c_state{cb_state = CB1});
      {stop, Reason, CB3} ->
         lager:notice("stop message from ~p node with reason ~p",[Module, Reason]),
         faxe:stop_task(State#c_state.graph_id, true),
         {noreply, State#c_state{cb_state = CB3}};
      {error, _Reason} ->
         {noreply, State#c_state{cb_state = CB}}
   end

;
handle_info(_Req, State=#c_state{cb_handle_info = false}) ->
   {noreply, State}
.

cb_handle_info(_Req, State = #c_state{cb_handle_info = false}) -> State;
cb_handle_info(Req, State = #c_state{cb_state = CB, component = Module}) ->
   case Module:handle_info(Req, CB) of
      {ok, CB0} ->
         State#c_state{cb_state = CB0};
      {error, _Reason} ->
         State
   end.

cb_handle_ack(_Mode, _DTag, State = #c_state{cb_handle_ack = false}) -> State;
cb_handle_ack(Mode, DTag, State = #c_state{cb_state = CB, component = Module}) ->
   case Module:handle_ack(Mode, DTag, CB) of
      {ok, CB0} ->
         State#c_state{cb_state = CB0};
      {error, _Reason} ->
         State
   end.

%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #c_state{}) -> term()).
terminate(_Reason, #c_state{}) ->
   ok.


-spec(code_change(OldVsn :: term() | {down, term()}, State :: #c_state{},
    Extra :: term()) ->
   {ok, NewState :: #c_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
setup_idle_stop(State, #{'_stop_idle' := StopIdle, '_idle_time' := IdleTime0}) ->
   %% stop flow on idle feature
   IdleTime = max(faxe_time:duration_to_ms(IdleTime0), ?MIN_IDLE_TIME),
   IdleCheckInterval = max(round(IdleTime/5), ?MIN_IDLE_CHECK_INTERVAL),
   case StopIdle of
      true -> erlang:send_after(IdleCheckInterval, self(), check_stop_on_idle);
      false -> ok
   end,

%%   lager:notice("stop on idle: ~p time: ~p interval: ~p",[StopIdle, IdleTime, IdleCheckInterval]),
   State#c_state{
      %% stop_flow_on_idle feature
      stop_on_idle = StopIdle,
      idle_time = IdleTime,
      idle_check_interval = IdleCheckInterval,
      idle_since = faxe_time:now()
   }.


-spec handle_process_result(tuple(), #c_state{}) -> {NewState::#c_state{}, boolean(), boolean()}.
handle_process_result({emit, Emitting, NState}, State=#c_state{}) when is_list(Emitting) ->
   [emit(Port, Emitted, State) || {Port, Emitted} <- Emitting],
   {State#c_state{cb_state = NState},false, true};
handle_process_result({emit, {Port, Emitted}, NState}, State=#c_state{}) when is_integer(Port) ->
   emit(Port, Emitted, State),
   {State#c_state{cb_state = NState},false, true};
handle_process_result({emit, Emitted, NState}, State=#c_state{}) ->
   emit(1, Emitted, State),
   {State#c_state{cb_state = NState},false, true};
handle_process_result({request, {Port, PPids}, NState}, State=#c_state{flow_mode = FMode}) when is_list(PPids) ->
   maybe_request_items(Port, PPids, FMode),
   {State#c_state{cb_state = NState},
      true, false};
handle_process_result({emit_ack, {Port, Emitted}, DTag, NState}, State = #c_state{inports = Ins}) ->
   emit(Port, Emitted, State),
   dataflow:ack(DTag, Ins),
   {State#c_state{cb_state = NState}, false, true};
handle_process_result({emit_ack, Emitted, DTag, NState}, State = #c_state{inports = Ins}) ->
   emit(1, Emitted, State),
   dataflow:ack(DTag, Ins),
   {State#c_state{cb_state = NState}, false, true};
handle_process_result({emit_request, {Port, Emitted}, {ReqPort, PPids}, NState},
    State=#c_state{flow_mode = FMode}) when is_list(PPids) ->
   emit(Port, Emitted, State),
   maybe_request_items(ReqPort, PPids, FMode),
   {State#c_state{cb_state = NState}, true, false};
handle_process_result({emit_request, Emitted, {ReqPort, PPids}, NState},
    State=#c_state{flow_mode = FMode}) when is_list(PPids) ->
   emit(1, Emitted, State),
   maybe_request_items(ReqPort, PPids, FMode),
   {State#c_state{cb_state = NState}, true, false};
handle_process_result({ok, NewCBState}, State=#c_state{}) ->
   {State#c_state{cb_state = NewCBState}, false, false};
handle_process_result({error, _What}, State=#c_state{}) ->
   metric(?METRIC_ERRORS, 1, State),
   exit(_What).

%%% @doc emits a value on a defined port
emit(Port, Value, State = #c_state{node_index = NodeIndex}) ->
   true = df_subscription:output(NodeIndex, Value, Port),
   metric(?METRIC_ITEMS_OUT, 1, State),
   maybe_debug(item_out, Port, Value, State).

%% @doc emit debug events
maybe_debug(_Key, _Port, _Value, #c_state{emit_debug = false}) ->
   ok;
maybe_debug(Key, Port, Value, #c_state{emit_debug = true, node_index = NodeIndex}) ->
   gen_event:notify(faxe_debug, {Key, NodeIndex, Port, Value}).


request_all(_Inports, push) ->
   ok;
request_all(Inports, pull) ->
   {_Ports, Pids} = lists:unzip(Inports),
   maybe_request_items(all, Pids, pull).
maybe_request_items(_Port, _Pids, push) ->
   ok;
maybe_request_items(Port, Pids, pull) ->
   dataflow:request_items(Port, Pids).

reset_idle(State, Tag) ->
%%   lager:info("reset idle time <~p>",[Tag]),
   State#c_state{idle_since = faxe_time:now()}.
%%%===================================================================
%%% PORTS for modules
%%%

inports() ->
   [{1, nil}].

outports() ->
   inports().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% METRICS %%%%%%%%%%%%%%%%%%%%%%%%%%%
metric(_Name, _Value, #c_state{flow_node_id = _NId}) ->
  ok.
%%   node_metrics:metric(NId, Name, Value).