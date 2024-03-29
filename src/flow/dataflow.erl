-module(dataflow).

-include("faxe_common.hrl").

%% API exports
-export([
   new_graph/0,
   create_graph/2,
   start_graph/1,
   start_graph/2,
   add_node/2,
   add_edge/2
]).


-export([
   request_items/2,
   emit/1,
   build_options/3,
   maybe_check_opts/2,
   maybe_debug/5,
   ack/2, ack/3]).

%%====================================================================
%% CALLBACK API functions
%%====================================================================

%% @doc get a new graph definition map
-spec new_graph() -> graph_definition().
new_graph() ->
   #{nodes => [], edges => []}.

%% @doc graph_definition() : add graph node
-spec add_node({any(), atom()} | {any(), atom(), list()}, graph_definition()) -> graph_definition().
add_node({NodeName, Component}, Defs) when is_atom(Component), is_map(Defs) ->
   add_node({NodeName, Component, []}, Defs);
add_node({NodeName, Component, Params}, Defs=#{nodes := Nodes})
   when is_atom(Component), is_map(Defs), is_map(Params) ->
   Defs#{nodes := [{NodeName, Component, Params} | Nodes]}.

%% @doc add a new edge to #{nodes := Nodes, edges := Edges}
-spec add_edge(tuple(), graph_definition()) -> graph_definition().
add_edge({NodeOut, PortOut, NodeIn, PortIn}, Defs) ->
   add_edge({NodeOut, PortOut, NodeIn, PortIn, []}, Defs);
add_edge({NodeOut, PortOut, NodeIn, PortIn, Params}, Defs = #{edges := Edges}) ->
   Defs#{edges := [{NodeOut, PortOut, NodeIn, PortIn, Params} | Edges]}.

%% @doc start a new df_graph process
-spec create_graph(any(), graph_definition()) -> {ok, pid()} | {error, Reason::any()}.
create_graph(Id, Definitions) when is_map(Definitions) ->
   lager:debug("start/create graph: ~p", [Id]),
   graph_sup:new(Id, Definitions).

%% @doc start the graph computation
start_graph(Graph) ->
   start_graph(Graph, push).
start_graph(Graph, #task_modes{} = TM) ->
   graph_starter:start_graph(Graph, TM); %#task_modes{run_mode = push});
start_graph(Graph, RunMode) ->
   graph_starter:start_graph(Graph, #task_modes{run_mode = RunMode}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% component functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
request_items(Port, PublisherPids) when is_list(PublisherPids) ->
%%   lager:notice("request item: ~p",[{Port, PublisherPids}]),
   [Pid ! {request, self(), Port} || Pid <- PublisherPids].

-spec emit(any()) -> reference().
emit(Value) ->
   emit(1, Value).
-spec emit(any(), non_neg_integer()) -> reference().
emit(Port, Value) ->
   erlang:send_after(0, self(), {emit, {Port, Value}}).

maybe_debug(_Key, _Port, _Value, _Idx, false) ->
   ok;
maybe_debug(Key, Port, Value, Idx, true) ->
   gen_event:notify(faxe_debug, {Key, Idx, Port, Value}).

-spec ack(undefined|non_neg_integer()|#data_point{}|#data_batch{}, list(tuple())) -> [ok].
ack(undefined, _Inputs) ->
   ok;
ack(DTag, Inputs) when is_integer(DTag)->
   ack(single, DTag, Inputs);
ack(DataItem, Inputs) when is_record(DataItem, data_point) orelse is_record(DataItem, data_batch)  ->
   {Mode, DTag} = retrieve_dtag(DataItem),
   ack(Mode, DTag, Inputs).

ack(_Mode, undefined, _Inputs) ->
   ok;
ack(Mode, DTag, Inputs) ->
   lists:foreach(fun({_Port, Pid}) -> Pid ! {ack, Mode, DTag} end, Inputs).

retrieve_dtag(#data_point{dtag = DTag}) ->
   {single, DTag};
retrieve_dtag(#data_batch{points = []}) ->
   {multi, 0};
retrieve_dtag(#data_batch{points = Points}) ->
   P = lists:last(Points),
   {multi, P#data_point.dtag}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_options(atom(), list( {atom(), option_value()} ), map()) -> map().
build_options(Component, L, Opts) ->
   case catch(do_build_options(Opts, L)) of
      Opts0 when is_map(Opts0) -> maybe_check_opts(Opts0, Component);
      {error, What} -> erlang:error({bad_option, {Component, What}});
      {'EXIT',{What, _}} -> erlang:error({bad_option, {Component, What}});
      Error -> erlang:error(format_error(bad_option, Component, Error))
   end.
do_build_options([], _) -> #{};
do_build_options(Opts, L) when is_list(L), is_list(Opts) ->
   %% precheck if all the given params make sense (exits for this node)
   build_eval(Opts, L, #{}).

build_eval([], _Given, Acc) ->
   Acc;
build_eval([{OptName, is_set}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_is_set(OptName, Given, Acc));
build_eval([{OptName, is_set, Default}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_is_set(OptName, Given, Default, Acc));
build_eval([{OptName, OptType, Default}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_opt(OptName, OptType, Default, Given, Acc));
build_eval([{OptName, OptType}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_opt(OptName, OptType, Given, Acc));
build_eval([#{name := OptName, type := is_set, default := Default}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_is_set(OptName, Given, Default, Acc));
build_eval([#{name := OptName, type := is_set}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_is_set(OptName, Given, Acc));
build_eval([#{name := OptName, type := Type, default := Default}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_opt(OptName, Type, Default, Given, Acc));
build_eval([#{name := OptName, type := Type}|Opts], Given, Acc) ->
   build_eval(Opts, Given, eval_opt(OptName, Type, Given, Acc));
build_eval(What, WhatGiven, Acc) ->
   lager:warning("unexpected clause build_eval(~p, ~p, ~p)",[What, WhatGiven, Acc]).

%%====================================================================
%% Internal functions
%%====================================================================
eval_is_set(OptName, Given, Acc) ->
   case proplists:get_value(OptName, Given) of
      true     -> Acc#{OptName => true};
      _  -> Acc#{OptName => false}
   end.
eval_is_set(OptName, Given, Default, Acc) ->
   case proplists:get_value(OptName, Given) of
      undefined -> Acc#{OptName => Default};
      true      -> Acc#{OptName => true};
      false      -> Acc#{OptName => false}
   end.
eval_opt(OptName, OptType, Given, Acc) ->
   case proplists:get_value(OptName, Given) of
      undefined ->
         throw([<<"option_missing: '">>,
            atom_to_binary(OptName,utf8), <<"', required type: ">>, atom_to_binary(OptType,utf8)]);
      V        -> Acc#{OptName => val(V, {OptName, OptType})}
   end.
eval_opt(OptName, OptType, Default, Given, Acc) ->
   case proplists:get_value(OptName, Given) of
      undefined -> Acc#{OptName => Default};
      V        -> Acc#{OptName => val(V, {OptName, OptType})}
   end.


%%%%%%%
-spec val(option_value(), {Name :: binary(), option_name()}) -> option_value().
val(Val, {OptName, duration}) when is_binary(Val) ->
   case is_duration(Val) of
      true -> Val;
      false -> option_error(<<"bad parameter type">>, Val, duration, OptName)
   end;
val(Val, {_, number}) when is_integer(Val) orelse is_float(Val) -> Val;
val(Val, {_, integer}) when is_integer(Val) -> Val;
val(Val, {_, int}) when is_integer(Val) -> Val;
val(Val, {_, pos_integer}) when is_integer(Val) andalso Val > 0 -> Val;
val(Val, {_, nonneg_integer}) when is_integer(Val) andalso Val > -1 -> Val;
val(Val, {_, float}) when is_float(Val) -> Val;
val(Val, {_, double}) when is_float(Val) -> Val;
val(Val, {_, binary}) when is_binary(Val) -> Val;
val(Val, {_, string}) when is_binary(Val) -> Val;
val(Val, {_, string_template}) when is_binary(Val) -> Val;
val(Val, {_, list}) when is_list(Val) -> Val;
val(Val, {_, tuple}) when is_tuple(Val) -> Val;
val(Val, {_, atom}) when is_atom(Val) -> Val;
val(Val, {_, lambda}) when is_record(Val, faxe_lambda) -> Val;
val(Val, {O, boolean}) -> val(Val, {O, bool});
val(true, {_, bool}) -> true;
val(false, {_, bool}) -> false;

val(Val, {N, number_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_integer(E) orelse is_float(E) end, numbers, N);
val(Val, {N, integer_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_integer(E) end, integers, N);
val(Val, {N, float_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_float(E) end, floats, N);
val(Val, {N, binary_list}) ->
   val(Val, {N, string_list});
val(Val, {N, string_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_binary(E) end, strings, N);
val(Val, {N, atom_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_atom(E) end, atoms, N);
val(Val, {N, lambda_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_record(E, faxe_lambda) end, lambdas, N);
val(Val, {N, duration_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_duration(E) end, durations, N);
val(Val, {N, tuple_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_tuple(E) end, tuples, N);

val(Val, {_, any}) -> Val;
val(V, {OptName, Type}) -> option_error(<<"bad parameter type">>, V, Type, OptName).

list_val([], _Fun, Type, OptName) ->
   option_error(<<"empty parameter(s)">>, "", Type, OptName);
list_val(Val, Fun, Type, OptName) ->
   case lists:all(Fun, Val) of
      true -> Val;
      false -> option_error(<<"bad parameter(s) type">>, Val, Type, OptName)
   end.

is_duration(Bin) -> faxe_time:is_duration_string(Bin).

%% further option checks
maybe_check_opts(Opts, Module) when is_map(Opts), is_atom(Module) ->
   case erlang:function_exported(Module, check_options, 0) of
      true -> check_options(Module:check_options(), Opts, Module);
      false -> Opts
   end.

check_options([], Opts, _Mod) ->
   Opts;
check_options([Check| Checks], Opts, Mod) ->
   do_check(Check, Opts, Mod),
   check_options(Checks, Opts, Mod).

do_check({same_length, [Key1|Keys]=_OptionKeys}, Opts = #{}, Mod) ->
%%   lager:warning("same_length check for ~p ~p out of ~p" ,[OptionKeys, Opts]),
   #{Key1 := L1} = Opts,
   L = erlang:length(L1),
   F = fun(KeyE) ->
      case maps:get(KeyE, Opts, undefined) of
         undefined -> ok;
         ListOpts ->
            case erlang:length(ListOpts) == L of
               true -> ok;
               false -> erlang:error(format_error(options_error, Mod,
                  [<<"Different parameter count for options '">>, atom_to_binary(Key1, utf8),
                     <<"' and '">>, atom_to_binary(KeyE, utf8), <<"'">>]))
            end
      end
       end,
   lists:foreach(F, Keys);

do_check({not_empty, Keys}, Opts, Mod) ->
   F = fun(KeyE) ->
      case maps:get(KeyE, Opts, undefined) of
         undefined -> ok; %% should not happen
         [] -> erlang:error(format_error(option_empty, Mod,
            [<<"Option may not be empty: '">>, atom_to_binary(KeyE, utf8), <<"'">>]));
         _ -> ok
      end
       end,
   lists:foreach(F, Keys);

do_check({max_param_count, Keys, Max}, Opts, Mod) ->
   F = fun(KeyE) ->
      case maps:get(KeyE, Opts, undefined) of
         undefined -> ok; %% should not happen
         Params -> case erlang:length(Params) > Max of
                      true -> erlang:error(format_error(too_many_items, Mod,
                         [<<"Max param count: '">>, Max, <<" for ">>,
                            atom_to_binary(KeyE, utf8), <<"'">>]));
                      false -> ok
                   end
      end
       end,
   lists:foreach(F, Keys);


%% check if at most one of the parameters is given
do_check({one_or_none_params, Keys}, Opts, Mod) ->
   do_check({check_one_params, Keys, true}, Opts, Mod);
%% check if exactly one of the required parameters is given
do_check({one_of_params, Keys}, Opts, Mod) ->
   do_check({check_one_params, Keys, false}, Opts, Mod);
do_check({check_one_params, Keys, AllowNone}, Opts, Mod) ->
   OptsKeys = maps:keys(Opts),
   Has =
      lists:filter(fun(E) ->
         lists:member(E, OptsKeys) andalso maps:get(E, Opts) /= undefined
                   end, Keys),
   case length(Has) of
      0 when AllowNone == true -> ok;
      1 -> ok;
      _ ->
         KeysBinList = ["'" ++ atom_to_list(K) ++ "'" || K <- Keys],
         erlang:error(format_error(invalid_opt, Mod,
            [<<"Must provide one of params: ">>, lists:join(<<", " >>, KeysBinList)]))
   end;

%% check if at least one of the required parameters is given
do_check({oneplus_of_params, Keys}, Opts, Mod) ->
%%   lager:notice("do check: ~p", [[{one_of_params, Keys}, Opts, Mod]]),
   OptsKeys = maps:keys(Opts),
   Has =
      lists:filter(fun(E) ->
         lists:member(E, OptsKeys) andalso maps:get(E, Opts) /= undefined
                   end, Keys),
%%   lager:notice("Has: ~p",[Has]),
   case length(Has) > 0 of
      true -> ok;
      _ ->
         KeysBinList = ["'" ++ atom_to_list(K) ++ "'" || K <- Keys],
         erlang:error(format_error(invalid_opt, Mod,
            [<<"Must provide at least one of params: ">>, lists:join(<<", " >>, KeysBinList)]))
   end;

%% check if one of possible values is given for a specific param
do_check({one_of, Key, ValidOpts}, Opts, Mod) ->
%%   lager:notice("do check: ~p",[[{one_of, Key, ValidOpts}, Opts, Mod]]),
   case maps:get(Key, Opts, undefined) of
      undefined -> ok; %% should not happen
      Params when is_list(Params) ->
%%         lager:warning("params: ~p", [Params]),
         lists:foreach(fun(E) ->
            case lists:member(E, ValidOpts) of
               true -> ok;
               false -> erlang:error(format_error(invalid_opt, Mod,
                  [<<"Cannot use '">>, E, <<"' for param '">>, atom_to_binary(Key, latin1),
                     <<"'">>, <<" must be one of: ">>, lists:join(<<", " >>, ValidOpts)]))
            end
                       end, Params);
      Param -> case lists:member(Param, ValidOpts) of
                  true -> ok;
                  false -> erlang:error(format_error(invalid_opt, Mod,
                     [<<"Cannot use '">>, Param, <<"' for param '">>, atom_to_binary(Key, latin1),
                        <<"'">>, <<" must be one of: ">>, lists:join(<<", " >>, ValidOpts)]))
               end
   end;
%% check options with a custom function
do_check({func, Key, Fun, Message}, Opts, Mod) when is_function(Fun), is_binary(Message)->
   Val = maps:get(Key, Opts, undefined),
%%   lager:notice("check func for :~p",[{Key, Val}]),
   Res =
      case erlang:fun_info(Fun, arity) of
         {arity, 1} -> Fun(Val);
         {arity, 2} -> Fun(Val, Opts)
      end,
   case Res of
      true -> ok;
      false -> erlang:error(format_error(invalid_opt, Mod,
         [<<"Param '">>, atom_to_binary(Key, latin1), <<"'">>, Message]));
      {false, MsgAddon} -> erlang:error(format_error(invalid_opt, Mod,
         [<<"Param '">>, atom_to_binary(Key, latin1), <<"'">>, Message, MsgAddon]))
   end;
do_check(_, _, Mod) ->
   erlang:error(format_error(invalid_check_opts, Mod, <<"unsupported_check_options_type">>)).


option_error(OptType, Given, Should, Name) ->
   throw([OptType,
      <<" given for param '">>,atom_to_binary(Name, utf8),<<"' ('">>,
      io_lib:format("~w",[Given]), <<"'), should be: ">>, atom_to_binary(Should, utf8)]).

format_error(Type, Component, Error) ->
   NodeName0 = atom_to_binary(Component, utf8),
   NodeName = binary:replace(NodeName0, <<"esp_">>, <<>>),
   iolist_to_binary(
      [atom_to_binary(Type, utf8), <<" for node ">>, NodeName, <<": ">>] ++ Error
   ).
