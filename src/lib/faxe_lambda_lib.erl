%% Date: 12.04.17 - 20:23
%% Ⓒ 2017 heyoka
%%
%% @doc
%% lambda function standard library
%%
-module(faxe_lambda_lib).
-author("Alexander Minichmair").

%% API
-compile(nowarn_export_all).
-compile(export_all).
-compile({no_auto_import,[map_get/2]}).
%%% @doc


%%% additional type checks
is_duration(Dur) -> faxe_time:is_duration_string(Dur).
is_boolean(In) -> is_bool(In).
is_bool(In) when In == true orelse In == false -> true;
is_bool(_) -> false.
is_int(In) -> erlang:is_integer(In).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% string functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% the module estr has several string manipulation functions, these can be used:

%%    str_at/2
%%,   str_capitalize/1
%%,   str_chunk/2
%%,   str_codepoints/1
%%,   str_contains/2
%%,   str_downcase/1
%%,   str_ends_with/2
%%,   str_ends_with_any/2
%%,   str_eqi/2
%%,   str_first/1
%%,   str_last/1
%%,   str_length/1
%%,   str_lstrip/1
%%,   str_lstrip/2
%%,   str_next_codepoint/1
%%,   str_normalize/2
%%,   str_pad_leading/2
%%,   str_pad_leading/3
%%,   str_pad_trailing/2
%%,   str_pad_trailing/3
%%,   str_replace/3
%%,   str_replace_leading/3
%%,   str_replace_prefix/3
%%,   str_replace_suffix/3
%%,   str_replace_trailing/3
%%,   str_reverse/1
%%,   str_rstrip/1
%%,   str_rstrip/2
%%,   str_slice/3
%%,   str_split/1
%%,   str_split/2
%%,   str_split/3
%%,   str_split_at/2
%%,   str_split_by_any/2
%%,   str_split_by_any/3
%%,   str_split_by_re/2
%%,   str_split_by_re/3
%%,   str_starts_with/2
%%,   str_starts_with_any/2
%%,   str_strip/1
%%,   str_strip/2
%%,   str_upcase/1
%%%%%%%%%%% additional str funcs
str_concat(String1, String2) ->
   unicode:characters_to_binary([String1, String2]).
str_concat(Strings) when is_list(Strings) ->
   unicode:characters_to_binary(Strings).

str_quote(String) when is_binary(String) ->
   <<"\"", String/binary, "\"">>.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Math functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% all function from erlang's 'math' module can be used in lambda-expressions
%% these are:

%% acos(X) -> float()
%%
%% acosh(X) -> float()
%%
%% asin(X) -> float()
%%
%% asinh(X) -> float()
%%
%% atan(X) -> float()
%%
%% atan2(Y, X) -> float()
%%
%% atanh(X) -> float()
%%
%% ceil(X) -> float()
%%
%% cos(X) -> float()
%%
%% cosh(X) -> float()
%%
%% exp(X) -> float()
%%
%% floor(X) -> float()
%%
%% fmod(X, Y) -> float()
%%
%% log(X) -> float()
%%
%% log10(X) -> float()
%%
%% log2(X) -> float()
%%
%% pow(X, Y) -> float()
%%
%% pi() -> float()
%%
%% sin(X) -> float()
%%
%% sinh(X) -> float()
%%
%% sqrt(X) -> float()
%%
%% tan(X) -> float()
%%
%% tanh(X) -> float()


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Mathex
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% all functions from the module mathex can be used
%% these are:

%% moving_average/1,
%% average/1,
%% sum/1,
%% stdev_sample/1,
%% stdev_population/1,
%% skew/1,
%% kurtosis/1,
%% variance/1,
%% covariance/2,
%% correlation/2,
%% correlation_matrix/1,
%% nth_root/2,
%% percentile/2,
%% zscore/1

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% std-lib
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% basic operators you can use in lambda expressions:
%% 'AND' -> " andalso ";
%% 'OR'  -> " orelse ";
%% '<='  -> " =< ";
%% '=>'  -> " >= ";
%% '!='  -> " /= ";
%% '!'   -> " not ";

%% dfs includes a std-lib, these functions are defined:

%%   type-conversions:
%%
%%   bool/1,
%%   int/1,
%%   float/1,
%%   string/1
%%
%%   some basic math funs:
%%
%%   abs/1,
%%   round/1,
%%   floor/1,
%%   min/2,
%%   max/2
%%
%% ... and much more
%%% @end

defined(Val) ->
   Val /= undefined.
undefined(Val) ->
   Val == undefined.

empty(undefined) -> true;
empty([]) -> true;
empty(<<>>) -> true;
empty(Val) when is_binary(Val) -> estr:str_strip(Val) == <<>>;
empty(_) -> false.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% additional string functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

topic_part(Topic, Index) when is_binary(Topic), is_integer(Index) ->
   topic_part(Topic, Index, <<"/">>).
topic_part(Topic, Index, Separator) when is_binary(Topic), is_binary(Separator), is_integer(Index) ->
   Parts = binary:split(Topic, Separator, [global, trim_all]),
   case (catch lists:nth(Index, Parts)) of
      Result when is_binary(Result) -> Result;
      _E ->
         erlang:error("faxe_lambda_lib, topic_part", [Topic, Index, Separator])
   end.

-spec to_json_string(map()|list()) -> binary().
to_json_string(MapOrList) when is_map(MapOrList) orelse is_list(MapOrList) ->
   case jiffy:encode(MapOrList, []) of
      Bin when is_binary(Bin) -> Bin;
      IoList when is_list(IoList) -> iolist_to_binary(IoList)
   end.

-spec from_json_string(binary()) -> map()|list().
from_json_string(Bin) when is_binary(Bin) ->
   try jiffy:decode(Bin, [return_maps]) of
      Json when is_map(Json) orelse is_list(Json) -> Json
   catch
      _:_ -> #{}
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% additional
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
round_float(Int, _Precision) when is_integer(Int) ->
   Int;
round_float(Float, Precision) when is_float(Float), is_integer(Precision) ->
   faxe_util:round_float(Float, Precision).

max([]) -> 0;
max(ValueList) when is_list(ValueList) ->
   lists:max(ValueList).
min([]) -> 0;
min(ValueList) when is_list(ValueList) ->
   lists:min(ValueList).

%% modulo function
modulo(X, Y) ->
   mod(X, Y).
mod(X, Y) ->
   faxe_time:mod(X, Y).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Time related functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get the current ms timestamp UTC
-spec now() -> non_neg_integer().
now() ->
   faxe_time:now().

to_date(Ts) -> faxe_time:to_date(Ts).

-spec to_date_string(non_neg_integer()) -> string().
to_date_string(Ts) ->
   {D,{Hour, Minute, Second, _Ms}} = to_date(Ts),
   qdate:to_string("Y-m-d h:ia", {D,{Hour, Minute, Second}}).

-spec to_iso8601(non_neg_integer()) -> binary().
to_iso8601(Ts) -> faxe_time:to_iso8601(Ts).
-spec to_rfc3339(non_neg_integer()) -> binary().
to_rfc3339(Ts) -> time_format:to_rfc3339(Ts).


dt_parse(Ts, Format) ->
   time_format:convert(Ts, Format).

%% @deprecated
time_convert(Ts, Format) ->
   time_format:convert(Ts, Format).

time_align(Ts, DurationUnit) when is_integer(Ts), is_binary(DurationUnit) ->
   Unit = faxe_time:binary_to_duration(DurationUnit),
   faxe_time:align(Ts, Unit).

from_duration(Dur) ->
   faxe_time:duration_to_ms(Dur).

-spec millisecond(non_neg_integer()) -> non_neg_integer().
millisecond(Ts) ->
   faxe_time:get(millisecond, Ts).

-spec second(non_neg_integer()) -> non_neg_integer().
second(Ts) ->
   faxe_time:get(second, Ts).

-spec minute(non_neg_integer()) -> non_neg_integer().
minute(Ts) ->
   faxe_time:get(minute, Ts).

-spec hour(non_neg_integer()) -> non_neg_integer().
hour(Ts) ->
   faxe_time:get(hour, Ts).

-spec day(non_neg_integer()) -> non_neg_integer().
day(Ts) ->
   faxe_time:get(day, Ts).

-spec day_of_week(non_neg_integer()) -> non_neg_integer().
day_of_week(Ts) ->
   faxe_time:get(day_of_week, Ts).

%%day_of_week_string(Ts) ->
%%   list_to_binary(faxe_time:get(day_of_week_string, Ts)).

-spec week(non_neg_integer()) -> non_neg_integer().
week(Ts) ->
   faxe_time:get(week, Ts).

-spec month(non_neg_integer()) -> non_neg_integer().
month(Ts) ->
   faxe_time:get(month, Ts).
%%month_string(Ts) ->
%%   list_to_binary(faxe_time:get(month_string, Ts)).

-spec year(non_neg_integer()) -> non_neg_integer().
year(Ts) ->
   faxe_time:get(year, Ts).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% random generators
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc generate a random integer uniformly distributed between 1 and N
-spec random(non_neg_integer()) -> non_neg_integer().
random(N) when is_integer(N), N > 0 ->
   rand:uniform(N).

%% @doc generate a random integer uniformly distributed between N and M
-spec random(pos_integer(), pos_integer()) -> pos_integer().
random(N, M) when is_integer(N), is_integer(M), N > 0, M > N ->
   N + random(M-N).

%% @doc generate a random float between 0.0 and 1.0, that gets multiplied by N
-spec random_real(non_neg_integer()) -> float().
random_real(N) ->
   rand:uniform_real() * N.

random_float(N) ->
   random_real(N).

%% @doc generate a standard normal deviate float (that is, the mean is 0 and the standard deviation is 1)
-spec random_normal() -> float().
random_normal() ->
   rand:normal().

%% @doc
%% Returns a random binary of size Length consisting of latins [a-zA-Z] and digits [0-9].
%% @end
-spec random_latin_string(pos_integer()) -> binary().
random_latin_string(Length) when is_integer(Length), Length > 0 ->
   faxe_util:random_latin_binary(Length).

%% @doc
%% Returns a random binary of size random(Min, Max) consisting of latins [a-zA-Z] and digits [0-9].
%% @end
-spec random_latin_string(pos_integer(), pos_integer()) -> binary().
random_latin_string(Min, Max) when is_integer(Min), is_integer(Max), Min > 0, Max > Min ->
   faxe_util:random_latin_binary(random(Min, Max)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% list/map functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% maps
-spec map_get(binary(), map()|binary()) -> term().
map_get(Key, Map) ->
   map_get(Key, Map, <<"undefined">>).

map_get(Key, JBin, Default) when is_binary(JBin) ->
   maps:get(Key, get_jsn(JBin), Default);
map_get(Key, Map, Default) when is_map(Map) ->
   maps:get(Key, Map, Default).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% json arrays
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

select_first(ReturnField, Mem) ->
   select_first(ReturnField, [], Mem).
select_first(ReturnField, Where, Mem) ->
   select_first(ReturnField, Where, Mem, undefined).
select_first(ReturnField, Where, Mem, Default) ->
   case select(ReturnField, Where, Mem, Default) of
      [undefined|_] -> erlang:error("faxe_lambda_lib:select_first returned undefined", [ReturnField, Where, Mem]);
      [Res|_] -> Res;
      Else -> Else
   end.


select_any(ReturnField, Where, Mem) ->
   select_any(ReturnField, Where, Mem, undefined).
select_any(_ReturnField, [], _Mem, Default) ->
   Default;
select_any(ReturnField, [Where|Conds], Mem, Default) ->
   case select(ReturnField, [Where], Mem, Default) of
      Default -> select_any(ReturnField, Conds, Mem, Default);
      [undefined|_] -> select_any(ReturnField, Conds, Mem, Default);
      undefined -> select_any(ReturnField, Conds, Mem, Default);
      [] -> select_any(ReturnField, Conds, Mem, Default);
      [Res|_] -> Res;
      Else -> Else
   end.

%% @doc
%% given a list of maps(json array), try to return all entries with the given key-value criteria (Where)
%% the Where param: either an empty list [], or a list of two-tuples
select(ReturnField, Mem) ->
   select(ReturnField, [], Mem).
select(ReturnField, Where, Mem) ->
   select(ReturnField, Where, Mem, undefined).
select(ReturnField, Where0, Mem0, Default) when is_binary(ReturnField), is_list(Where0) ->
   Where = prepare_conditions(Where0, []),
   H = erlang:phash2({ReturnField, Where, Mem0, Default}),
   case select_cache(H) of
      false ->
         Res = do_select(ReturnField, Where, Mem0, Default),
         catch ets:insert(select_cache, {H, Res}),
         Res;
      CachedRes -> CachedRes
   end.

prepare_conditions([], Acc) -> Acc;
prepare_conditions([{_Path, _Pattern} = Cond|Conditions], Acc) ->
   prepare_conditions(Conditions, Acc ++ [Cond]);
prepare_conditions([{<<"regex">>, Path, Pattern}|Conditions], Acc) ->
   prepare_conditions(Conditions, prepare_cond_fun(Path, Pattern, Acc));
prepare_conditions([{regex, Path, Pattern}|Conditions], Acc) ->
   prepare_conditions(Conditions, prepare_cond_fun(Path, Pattern, Acc)).

prepare_cond_fun(Path, Pattern, Acc) ->
   CondFun =
      fun(Val) ->
         case re:run(Val, Pattern, []) of
            nomatch -> false;
            {match, _} -> true
         end
      end,
   Acc ++ [{Path, CondFun}].




select_all(_ReturnField, Where, Mem) ->
   select_all(_ReturnField, Where, Mem, []).

select_all(_ReturnField, [], _Mem0, Default) ->
   Default;
select_all(ReturnField, Where, Mem0, Default) ->
   select_all(ReturnField, Where, Mem0, Default, []).

select_all(_ReturnField, [], _Mem0, Default, []) ->
   Default;
select_all(_ReturnField, [], _Mem0, _Default, Results) ->
   Results;
select_all(ReturnField, [Where|Conds], Mem0, Default, Results) ->
   case select(ReturnField, [Where], Mem0, Default) of
      [] -> select_all(ReturnField, Conds, Mem0, Default, Results);
      Res when is_list(Res) -> select_all(ReturnField, Conds, Mem0, Default, Results++Res)
   end.

do_select(ReturnField, Where, Mem0, Default) when is_binary(ReturnField), is_list(Where) ->
   Mem = get_jsn(Mem0),
   case jsn:select({value, ReturnField, Default}, Where, Mem) of
      Res when is_list(Res) -> Res;
      undefined -> erlang:error("faxe_lambda_lib select returned undefined", [ReturnField, Where, Mem])
   end.


select_cache(H) ->
   case catch lookup_select(H) of
      V when is_list(V) -> V;
      _ ->
         false
   end.

%% based on type, return a list or map structure, possibly from a json string (cached)
-spec get_jsn(binary()|list()|map()) -> list()|map().
get_jsn(Mem) when is_binary(Mem) ->
   H = erlang:phash2(Mem),
   case catch lookup_json(H) of
      V when is_list(V) orelse is_map(V) ->
         V;
      _ ->
         Decoded = from_json_string(Mem),
         catch ets:insert(decoded_json, {H, Decoded}),
         Decoded
   end;
get_jsn(Mem) when is_list(Mem) orelse is_map(Mem) ->
   Mem.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% lambda state functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
lookup_select(Hash) ->
   case ets:lookup(select_cache, Hash) of
      [{Hash, Val}] -> Val;
      [] -> undefined;
      Other -> Other
   end.

lookup_json(Hash) ->
   case ets:lookup(decoded_json, Hash) of
      [{Hash, Val}] -> Val;
      [] -> undefined;
      Other -> Other
   end.

%% get a value from a mem (ets) node
ls_mem_list(Key) ->
   mem_lookup(Key).
ls_mem_set(Key) ->
   mem_lookup(Key).
ls_mem(Key) ->
   mem_lookup(Key).
mem(Key) ->
   mem_lookup(Key).
mem_lookup(Key) ->
   case graph_node_registry:get_graph_table() of
      {error, _What} -> lager:notice("could not find graph ets table: ~p for ~p",[{Key, self()}]), [];
      Tab ->
      Res =
         case ets:lookup(Tab, Key) of
            [{Key, Val}] -> Val;
            Other -> Other
         end,
      Res
   end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%% side load
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%sideload_dh(<<"topics">>, SiteId) ->
%%   sideload_dh(<<"topics">>, SiteId, undefined).
%%sideload_dh(<<"topics">>, SiteId, DeviceId) ->
%%   sideload_dh(<<"topics">>, SiteId, DeviceId, undefined).
%%sideload_dh(<<"topics">>, SiteId, DeviceId, DataFormat) ->
%%   do_me_the_graphql_query().


