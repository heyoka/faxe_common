%% Date: 07.01.17 - 22:41
%% Ⓒ 2017 heyoka
%%
%% @doc
%% This module provides functions for all needs in context with #data_point and #data_batch records.
%%
%% -record(data_point, {
%%    ts                :: non_neg_integer(), %% timestamp in ms
%%    fields   = #{}    :: map(),
%%    tags     = #{}    :: map(),
%%    id       = <<>>   :: binary()
%% }).
%%
%% -record(data_batch, {
%%    id                :: binary(),
%%    points            :: list(#data_point{}),
%%    start             :: non_neg_integer(),
%%    ed                :: non_neg_integer()
%% }).
%%
%% The basic data_type for the record-fields 'fields' and 'tags' is a map.
%% Note: for 'tags' it is assumed, that the map is 1-dimensional
%%
%% Field and Tag names in a data_point are always binary strings.
%% Field values are simple int, string or float values, but can also be deep nested maps and lists
%%
%% Tag values a always binary strings.
%%
%% Every data_point record has a ts-field it's value is always a unix-timestamp in
%% millisecond precision.
%%
%% For every function that expects a name/path (binary) a jsonpath query can be provided, ie:
%%
%% * flowdata:field(#data_point{}, <<"value.sub[2].data">>). will return the value at value.sub[2].data
%% * flowdata:field(#data_point{}, <<"averages">>).
%% * flowdata:field(#data_point{}, <<"averages.emitted[5]">>).
%%
%% A data_batch record consists of an ordered (by timestamp) list of data_point records (field 'points').
%% The 'points' list is ordered by timestamp such that the oldest point is the last entry in the list.
%% @end

-module(flowdata).
-author("Alexander Minichmair").

-include("faxe_common.hrl").

%% API
-export([
   %% batch and point
   ts/1, to_json/1,
   field/2,
   tag/2,
   id/1,
   set_field/3,
   set_tag/3,
   value/2, values/2, delete_field/2, delete_tag/2,
   first_ts/1, set_bounds/1,
   set_ts/2,
   field_names/1, tag_names/1,
   rename_fields/3, rename_tags/3,
   expand_json_field/2, extract_map/2, extract_field/3,
   field/3, to_s_msgpack/1, from_json/1,
   to_map/1, set_fields/3, set_tags/3, fields/2, tags/2,
   delete_fields/2, delete_tags/2, path/1, paths/1, set_fields/2,
   to_mapstruct/1, from_json_struct/1, from_json_struct/3, from_json_struct/4,
   point_from_json_map/1, point_from_json_map/3,
   set_tags/2, is_root_path/1,
   merge_points/1, merge/2, merge_points/2
%%   ,
%%   get_schema/1
   , clean_field_keys/1,
   to_map_except/2,
   new/0,
   new/1,
   set_root/2,
   set_root_key/2,
   tss_fields/2,
   values/3,
   value/3,
   ts/2,
   merge/1,
   set_dtag/2,
   to_num/1, to_num/2, with/2, fields/3, tss_fields/3]).
-export([convert_path/1]).

-define(DEFAULT_FIELDS, [<<"id">>, <<"df">>, <<"ts">>]).
-define(DEFAULT_TS_FIELD, <<"ts">>).
-define(DTAG_FIELD, <<"dtag">>).

-define(DOT_ESCAPE, <<"*">>).


-spec to_json(#data_point{} | #data_batch{}) -> binary().
to_json(P) when is_record(P, data_point) orelse is_record(P, data_batch) ->
   case jiffy:encode(to_mapstruct(P), []) of
      Bin when is_binary(Bin) -> Bin;
      IoList when is_list(IoList) -> iolist_to_binary(IoList)
   end.
%%
%% empty, when there is no field set (empty map)
to_mapstruct(#data_point{ts = _Ts, fields = Fields, tags = _Tags}) when map_size(Fields) == 0 ->
   #{};
to_mapstruct(_P=#data_point{ts = Ts, fields = Fields, tags = _Tags}) ->
   Fields#{?DEFAULT_TS_FIELD => Ts};
to_mapstruct(_B=#data_batch{points = Points}) ->
   [to_mapstruct(P) || P <- Points].

-spec to_s_msgpack(P :: #data_point{}|#data_batch{}) -> binary() | {error, {badarg, term()}}.
to_s_msgpack(P) when is_record(P, data_point) orelse is_record(P, data_batch) ->
   msgpack:pack(to_mapstruct(P), [{map_format, jiffy}]).


from_json(Message) ->
   try jiffy:decode(Message, [return_maps, dedupe_keys]) of
      Json when is_map(Json) orelse is_list(Json) -> Json
   catch
      _:_ -> #{}
   end.

-spec from_json_struct(binary()) -> #data_point{}|#data_batch{}.
from_json_struct(JSON) ->
   from_json_struct(JSON, ?DEFAULT_TS_FIELD, ?TF_TS_MILLI).

-spec from_json_struct(binary(), binary(), binary()) -> #data_point{}|#data_batch{}.
from_json_struct(JSON, TimeField, TimeFormat) ->
   from_json_struct(JSON, TimeField, TimeFormat, false).
-spec from_json_struct(binary(), binary(), binary(), true|false) -> #data_point{}|#data_batch{}.
from_json_struct(JSON, TimeField, TimeFormat, CleanFieldNames) ->
   Struct = from_json(JSON),
   build_item(CleanFieldNames, Struct, TimeField, TimeFormat).

build_item(false, Map, TimeField, TimeFormat) when is_map(Map) ->
   point_from_json_map(Map, TimeField, TimeFormat);
build_item(true, Map, TimeField, TimeFormat) when is_map(Map) ->
   point_from_json_map(clean_field_keys(Map), TimeField, TimeFormat);
build_item(false, List, TimeField, TimeFormat) when is_list(List) ->
   Points = [point_from_json_map(PMap, TimeField, TimeFormat) || PMap <- List],
   #data_batch{points = Points};
build_item(true, List, TimeField, TimeFormat) when is_list(List) ->
   Points = [point_from_json_map(clean_field_keys(PMap), TimeField, TimeFormat) || PMap <- List],
   #data_batch{points = Points}.


%% replace dots with underscores in field keys
clean_field_keys(Map) when is_map(Map) ->
   F = fun(Key, Val, Acc) ->
      NewKey = binary:replace(Key, <<".">>, <<"_">>, [global]),
      Acc#{NewKey => Val}
       end,
   maps:fold(F, #{}, Map).

%% from a map get a data_point record
-spec point_from_json_map(map()) -> #data_point{}.
point_from_json_map(Map) ->
   point_from_json_map(Map, ?DEFAULT_TS_FIELD, ?TF_TS_MILLI).

-spec point_from_json_map(map(), binary(), binary()) -> #data_point{}.
point_from_json_map(Map, ?DEFAULT_TS_FIELD, ?TF_TS_MILLI) ->
   Ts = maps:get(?DEFAULT_TS_FIELD, Map, faxe_time:now()),
   DTag = maps:get(?DTAG_FIELD, Map, undefined),
   #data_point{ts = Ts, dtag = DTag, fields = maps:without([?DEFAULT_TS_FIELD, ?DTAG_FIELD], Map)};
point_from_json_map(Map, TimeField, TimeFormat) ->
   Ts0 = jsn_get(TimeField, Map, undefined),
   Ts =
      case Ts0 of
         undefined -> faxe_time:now();
         Timestamp -> time_format:convert(Timestamp, TimeFormat)
      end,
   Fields = maps:without([?DEFAULT_TS_FIELD, ?DTAG_FIELD], Map),
   #data_point{ts = Ts, fields = Fields, dtag = maps:get(?DTAG_FIELD, Map, undefined)}.


%% return a pure map representation from a data_point/data_batch, adds a timestamp as <<"ts">>
-spec to_map(#data_point{}|#data_batch{}) -> map()|list(map()).
to_map(#data_point{ts = Ts, fields = Fields, tags = Tags}) ->
   M = maps:merge(Fields, Tags),
   M#{?DEFAULT_TS_FIELD => Ts};
to_map(#data_batch{points = Points}) ->
   [to_map(P) || P <- Points].

%% return a map representation of #data_point without the given keys
to_map_except(P=#data_point{}, Without) when is_list(Without) ->
   maps:without([[?DEFAULT_TS_FIELD]|Without], to_map(P)).

%% extract a given map into the fields-list in data_point P
%% return the updated data_point
-spec extract_map(#data_point{}, map()) -> #data_point{}.
extract_map(P = #data_point{fields = Fields}, Map) when is_map(Map) ->
   List = maps:to_list(Map),
   P#data_point{fields = Fields ++ List}.

expand_json_field(P = #data_point{}, FieldName) ->
   JSONVal = field(P, FieldName),
   P0 = delete_field(P, FieldName),
   Map = jiffy:decode(JSONVal, [return_maps]),
   P0#data_point{fields = P0#data_point.fields ++ Map}.

%% @doc merge a list of data_points into one
merge_points([#data_point{ts=Ts} |_Ps] = Points, Field) ->
   FMaps = [flowdata:field(P, Field) || P <- Points],
   Res = merge_fields(FMaps),
   set_field(#data_point{ts = Ts}, Field, Res).
merge_points([#data_point{ts=Ts} |_Ps] = Points) ->
   FMaps = [P#data_point.fields || P <- Points],
   Res = merge_fields(FMaps),
   #data_point{ts = Ts, fields = Res}.
merge_fields(FieldMaps) ->
   merge(FieldMaps).
%%   merge_fields(FieldMaps, #{}).
%%merge_fields([], Acc) ->
%%   Acc;
%%merge_fields([F1|Fields], Acc) ->
%%   merge_fields(Fields, merge(F1, Acc)).

%% get an empty data_point or data_batch record
-spec new() -> #data_point{}.
new() ->
   new(data_point).
-spec new(data_point|data_batch) -> #data_point{}|#data_batch{}.
new(data_point) ->
   #data_point{ts = faxe_time:now()};
new(data_batch) ->
   #data_batch{}.

%% @doc
%% get the timestamp from the given field
%% @end
-spec ts(#data_point{}|#data_batch{}) -> non_neg_integer() | list(non_neg_integer()).
ts(#data_point{ts = Ts}) ->
   Ts
;
%% @doc
%% get a list of all timestamps from all fields in the data_batch
%% @end
ts(#data_batch{points = Points}) ->
   [ts(Point) || Point <- Points].

%% @doc
%% get a data_point's timestamp. If it is not defined, uses a default instead.
%% @end
-spec ts(#data_point{}|#data_batch{}, Default :: non_neg_integer()) -> non_neg_integer() | list(non_neg_integer()).
ts(#data_point{ts = undefined}, Default) ->
   Default;
ts(#data_point{ts = Ts}, _Default) ->
   Ts;
ts(#data_batch{points = Points}, Default) ->
   [ts(P, Default) || P <- Points].

%%
%% @doc
%% get the value with a key from a #data_point field or tag,
%%
%% or get the points timestamp (this is used when no context for processing is given,
%% ie.: in a lambda fun)
%% @end
%%
-spec value(#data_point{}, jsonpath:path()) -> term()|undefined.
value(P = #data_point{}, F) ->
   value(P, F, undefined).
-spec value(#data_point{}, jsonpath:path(), Default :: term()) -> term()|undefined.
value(#data_point{ts = Ts}, ?DEFAULT_TS_FIELD, _Default) ->
   Ts;
value(#data_point{fields = Fields, tags = Tags}, F, Default) ->
   Path = path(F),
   case jsn_get(Path, Fields, Default) of
      undefined -> jsn_get(Path, Tags, Default);
      Value -> Value
   end.
%%
%% @doc get the values with a key from data_batch fields or tags @end
%%
values(B = #data_batch{}, F) ->
   values(B, F, undefined).
-spec values(#data_batch{}, jsonpath:path(), Default :: term()) -> list(term()|undefined).
values(#data_batch{points = Points}, F, Default) ->
   [value(Point, F, Default) || Point <- Points].

%%
%% @doc get the values with a key from field(s) @end
%%
-spec field(#data_point{}|#data_batch{}, binary(), term()) -> term().
field(#data_point{fields = Fields}, F, _Default) when is_map_key(F, Fields) ->
   maps:get(F, Fields);
field(#data_point{fields = Fields}, F, Default) ->
   jsn_get(F, Fields, Default)
;
field(#data_batch{points = Points}, F, Default) ->
   [field(Point, F, Default) || Point <- Points].

-spec field(#data_point{}|#data_batch{}, jsonpath:path()) -> undefined | term() | list(term()).
field(#data_point{fields = Fields}, F) when is_map_key(F, Fields) ->
   maps:get(F, Fields);
field(#data_point{fields = Fields}, F) ->
   jsn_get(F, Fields)
;
field(#data_batch{points = Points}, F) ->
   [field(Point, F) || Point <- Points].

%% @doc get a list of field-values with a list of keys/paths
fields(#data_point{fields = _Fields}, []) ->
   [];
fields(#data_point{fields = Fields}, PathList) when is_list(PathList) ->
   jsn_getlist(PathList, Fields).

%% @doc get a list of field-values with a list of keys/paths and a default value for all list entries
fields(#data_point{fields = _Fields}, [], _Default) ->
   [];
fields(#data_point{fields = Fields}, PathList, Default) when is_list(PathList) ->
   jsn_getlist(PathList, Fields, Default).

%% like maps:with but for deeply nested data
-spec with(#data_point{}|#data_batch{}, list()) -> #data_point{}|#data_batch{}.
with(#data_point{fields = Fields} = P, PathList) when is_list(PathList) ->
   Kept = jsn_with(PathList, Fields),
   P#data_point{fields = Kept};
with(#data_batch{points = Points} = B, PathList) when is_list(PathList) ->
   NewPoints = [with(P, PathList) || P <- Points],
   B#data_batch{points = NewPoints}.

%% get the timestamps and fieldvalues  as {TsList, ValList} from a data_batch, where undefineds are not included
-spec tss_fields(#data_batch{}, binary()|tuple(), IsRootPath :: true|false) -> {list(), list()}.
tss_fields(B=#data_batch{}, Paths) when is_list(Paths) ->
   tss_fields(B, Paths, false);
tss_fields(B=#data_batch{}, Path) ->
   tss_fields(B, Path, false).
tss_fields(B=#data_batch{}, Paths, true) when is_list(Paths) ->
   [tss_fields(B, F, true) || F <- Paths];
tss_fields(#data_batch{points = Points}, Path, true) ->
   lists:foldl(
     fun
        (#data_point{ts = Ts, fields = #{Path := Val}}, {Tss, Vals}) ->
           {Tss++[Ts], Vals++[Val]};
        (_, Acc) ->
           Acc
     end,
      {[],[]},
      Points
   );
tss_fields(B=#data_batch{}, Paths, false) when is_list(Paths) ->
   [tss_fields(B, F, false) || F <- Paths];
tss_fields(#data_batch{points = Points}, Path, false) ->
   lists:foldl(
      fun(Point=#data_point{ts=Ts}, {Tss, Vals} =Acc) ->
         case field(Point, Path) of
            undefined -> Acc;
            Val -> {Tss++[Ts], Vals++[Val]}
         end
      end,
      {[], []},
      Points
   ).



%% @doc
%% get an unordered list of all fieldnames from the given data_point
%% @end
-spec field_names(#data_point{}) -> list().
field_names(#data_point{fields = Fields}) ->
   maps:keys(Fields).

%% @doc
%% get an unordered list of all tag-names from the given data_point
%% @end
-spec tag_names(#data_point{}) -> list().
tag_names(#data_point{tags = Tags}) ->
   maps:keys(Tags).

%%%% setter

%% @doc
%% set the timestamp for the given data_point
%% @end
-spec set_ts(#data_point{}, non_neg_integer()) -> #data_point{}.
set_ts(P=#data_point{}, NewTs) ->
   P#data_point{ts = NewTs}.


%% @doc
%% set a dtag (some kind of deliver tag maybe) to a data-item
%% @end
set_dtag(P = #data_point{}, DTag) ->
   P#data_point{dtag = DTag};
set_dtag(B = #data_batch{}, DTag) ->
   B#data_batch{dtag = DTag}.

%% @doc
%% Set the field Key to Value with path.
%% If a field named Key does exist, it will be overwritten.
%% @end
-spec set_field(#data_point{}, jsonpath:path(), any()) -> #data_point{}.
%% special version timestamp field
set_field(P = #data_point{}, ?DEFAULT_TS_FIELD, Value) ->
   P#data_point{ts = Value}
;
set_field(P = #data_point{fields = Fields}, Key, Value) ->
   NewFields = set(Key, Value, Fields),
   P#data_point{fields = NewFields}
;
%%%
%%% @doc
%%% set field with name Key to every Value in the databatch
%%% @end
%%
set_field(P = #data_batch{points = Points}, Key, Value) ->
   Ps = lists:map(
     fun(#data_point{} = D) ->
        set_field(D, Key, Value)
     end,
      Points
   ),
   P#data_batch{points = Ps}.

%% set multiple fields at once
-spec set_fields(#data_point{}|#data_batch{}, list(), list()) -> #data_point{}|#data_batch{}.
set_fields(P = #data_point{fields = Fields}, Keys, Values) when is_list(Keys), is_list(Values) ->
   NewFields = jsn_setlist(Keys, Values, Fields),
   P#data_point{fields = NewFields};
set_fields(B = #data_batch{points = Points}, Keys, Values) when is_list(Keys), is_list(Values) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_fields(D, Keys, Values)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

set_fields(P = #data_point{fields = Fields}, KeysValues) when is_list(KeysValues) ->
   NewFields = jsn_setlist(KeysValues, Fields),
   P#data_point{fields = NewFields};
set_fields(B = #data_batch{points = Points}, KeysValues) when is_list(KeysValues) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_fields(D, KeysValues)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc 
%% set a key value pair into a fieldlist (which is a map)
%% if an entry with Key exists already, then the entry will be updated,
%% otherwise the entry will be appended to the fieldlist
%% @end
-spec set(jsonpath:path(), term(), list()) -> list().
set(Key, Value, FieldList) ->
   case is_root_path(Key) of
      true -> FieldList#{Key => Value};
      false -> jsn_set(Key, Value, FieldList)
   end.

-spec tag(#data_point{}|#data_batch{}, jsonpath:path()) -> undefined | term() | list(term()|undefined).
%%% @doc
%%% get a tags' value
%%% @end
tag(#data_point{tags = Fields}, F) ->
   jsn_get(F, Fields)
;
tag(#data_batch{points = Points}, F) ->
   [tag(Point, F) || Point <- Points].

%% @doc get a list of field-values with a list of keys/paths
tags(#data_point{tags = Tags}, PathList) when is_list(PathList) ->
   jsn_getlist(PathList, Tags).

%% @doc
%% get the id from the given data_point or data_batch
%% @end
-spec id(#data_batch{}) -> any().
id(#data_batch{id = Id}) ->
   Id;
id(#data_point{id = Id}) ->
   Id.

%% @doc
%% set a tag to a given value, or set all tags in all points to a given value
%% @end
-spec set_tag(#data_batch{}|#data_point{}, binary(), binary()) -> #data_batch{}|#data_point{}.
set_tag(P = #data_point{tags = Fields}, Key, Value) ->
   NewFields = set(Key, Value, Fields),
   P#data_point{tags = NewFields}
;
set_tag(B = #data_batch{points = Points}, Key, Value) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_tag(D, Key, Value)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% set multiple tags at once
-spec set_tags(#data_point{}|#data_batch{}, list(), list()) -> #data_point{}|#data_batch{}.
set_tags(P = #data_point{tags = Tags}, Keys, Values) when is_list(Keys), is_list(Values) ->
   NewTags = jsn_setlist(Keys, Values, Tags),
   P#data_point{tags = NewTags};
set_tags(B = #data_batch{points = Points}, Keys, Values) when is_list(Keys), is_list(Values) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_tags(D, Keys, Values)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

set_tags(P = #data_point{tags = Tags}, KeysValues) when is_list(KeysValues) ->
   NewTags = jsn_setlist(KeysValues, Tags),
   P#data_point{tags = NewTags};
set_tags(B = #data_batch{points = Points}, KeysValues) when is_list(KeysValues) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_tags(D, KeysValues)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc
%% delete a field with the given name, if a data_batch record is provided, the field gets deleted from all
%% data_points in the data_batch
%% @end
-spec delete_field(#data_point{}|#data_batch{}, binary()) -> #data_point{} | #data_batch{}.
delete_field(#data_point{fields = Fields}=P, FieldName) when is_map_key(FieldName, Fields) ->
   P#data_point{fields = maps:remove(FieldName, Fields)};
delete_field(#data_point{fields = Fields}=P, FieldName) ->
   case field(P, FieldName) of
      undefined -> P;
      _Else -> JData = jsn_delete(FieldName, Fields),
         P#data_point{fields = JData}
   end
   ;
delete_field(#data_batch{points = Points}=B, FieldName) ->
   NewPoints = [delete_field(Point, FieldName) || Point <- Points],
   B#data_batch{points = NewPoints}.

%% @doc delete a list of keys/paths
-spec delete_fields(#data_point{}|#data_batch{}, list()) -> #data_point{}|#data_batch{}.
delete_fields(P = #data_point{fields = Fields}, KeyList) when is_list(KeyList) ->
   NewFields = jsn_deletelist(KeyList, Fields),
   P#data_point{fields = NewFields};
delete_fields(B = #data_batch{points = Points}, KeyList) when is_list(KeyList) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         delete_fields(D, KeyList)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc
%% delete a tag with the given name, if a data_batch record is provided, the tag gets deleted from all
%% data_points in the data_batch
%% @end
-spec delete_tag(#data_point{}|#data_batch{}, binary()) -> #data_point{} | #data_batch{}.
delete_tag(#data_point{tags = Tags}=P, TagName) ->
   P#data_point{tags = jsn_delete(TagName, Tags)}
;
delete_tag(#data_batch{points = Points}=B, TagName) ->
   NewPoints = [delete_tag(Point, TagName) || Point <- Points],
   B#data_batch{points = NewPoints}.

%% @doc delete a list of keys/paths from tags
-spec delete_tags(#data_point{}|#data_batch{}, list()) -> #data_point{}|#data_batch{}.
delete_tags(P = #data_point{tags = Tags}, KeyList) when is_list(KeyList) ->
   NewTags = jsn_deletelist(KeyList, Tags),
   P#data_point{tags = NewTags};
delete_tags(B = #data_batch{points = Points}, KeyList) when is_list(KeyList) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         delete_tags(D, KeyList)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc
%% rename fields and tags, does not insert any value, when field(s) / tag(s) are not found
%% @end
-spec rename_fields(#data_point{}, list(jsonpath:path()), list(jsonpath:path())) -> #data_point{}.
rename_fields(#data_point{fields = Fields} = P, FieldNames, Aliases) ->
   P#data_point{fields = rename(Fields, lists:reverse(FieldNames), lists:reverse(Aliases))}
;
rename_fields(#data_batch{points = Points} = B, FieldNames, Aliases) ->
   NewPoints = [rename_fields(P, FieldNames, Aliases) || P <- Points],
   B#data_batch{points = NewPoints}.

rename_tags(#data_point{tags = Tags} = P, TagNames, Aliases) ->
   P#data_point{tags = rename(Tags, lists:reverse(TagNames), lists:reverse(Aliases))}.

rename(Map, [], []) ->
   Map;
rename(Map, [From|RFrom], [To|RTo]) when is_map(Map) ->
   NewData = do_rename(Map, From, To),
   rename(NewData, RFrom, RTo).

do_rename(Map, <<>>, To) ->
   jsn_set(To, Map, #{});
do_rename(Map, From, To) ->
   Val = jsn_get(From, Map, undefined),
   case Val of
      undefined -> Map;
      _Val -> NewData = jsn_delete(From, Map),
         set(To, Val, NewData)
   end.

%% @doc searches for a path-value and returns a new data_point with this value
%% if the given path is not found, returns a Default value
extract_field(P = #data_point{}, Path, FieldName) ->
   extract_field(P, Path, FieldName, 0).

-spec extract_field(#data_point{}, binary(), binary()) -> #data_point{}.
extract_field(P = #data_point{}, Path, FieldName, Default) ->
   NewValue =
   case field(P, Path) of
      undefined -> Default;
      Val -> Val
   end,
   set_field(#data_point{}, FieldName, NewValue).
   


%%%%%%%%%%%%%%%%%%%%%%%% batch only  %%%%%%%%%%%%%%%%%%

set_bounds(B=#data_batch{points = [F|_R]}) ->
   set_first(B#data_batch{ed = ts(F)}).
set_first(B=#data_batch{points = Ps}) ->
   P = lists:last(Ps),
   B#data_batch{start = ts(P)}.

%% @doc
%% get the first (ie oldest) timestamp from a data_batch
%% @end
-spec first_ts(#data_batch{}) -> non_neg_integer().
first_ts(#data_batch{points = []}) ->
   undefined;
first_ts(#data_batch{points = P}) ->
   ts(lists:last(P)).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% paths
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
jsn_get(_Path, Map) when map_size(Map) == 0 ->
   undefined;
jsn_get(Path, Map) when is_tuple(Path) ->
   jsn:get(Path, Map);
jsn_get(Path, Map) ->
   jsn:get(path(Path), Map).
jsn_get(_Path, Map, Default) when map_size(Map) == 0 ->
   Default;
jsn_get(Path, Map, Default) ->
   jsn:get(path(Path), Map, Default).

jsn_getlist(PathList, M) ->
   jsn_getlist(PathList, M, undefined).

jsn_getlist(_PathList, Empty, _Def) when map_size(Empty) == 0  ->
   [];
jsn_getlist(PathList, Map, Default) when is_list(PathList) ->
   jsn:get_list(paths(PathList), Map, Default).

jsn_set(Path, Val, Map) ->
   jsn:set(path(Path), Map, Val).
jsn_setlist(Keys, Values, Map) when is_list(Keys), is_list(Values) ->
   jsn:set_list(lists:zip(paths(Keys), Values), Map).
jsn_setlist([], Map) ->
   Map;
%% is the path a tuple already? then do not convert paths
jsn_setlist([{K, _V}|_]=KeysValues, Map) when is_tuple(K) ->
   jsn:set_list(KeysValues, Map);
jsn_setlist(KeysValues, Map) when is_list(KeysValues) ->
   {Keys, Values} = lists:unzip(KeysValues),
   jsn_setlist(Keys, Values, Map).

jsn_delete(_Path, Map) when map_size(Map) == 0 ->
   Map;
jsn_delete(Path, Map) ->
   jsn:delete(path(Path), Map).

jsn_deletelist([], Map) ->
   Map;
jsn_deletelist(_Paths, Map) when map_size(Map) == 0 ->
   Map;
jsn_deletelist(Paths, Map) ->
   jsn:delete_list(paths(Paths), Map).

jsn_with([], Map) ->
   Map;
jsn_with(_Paths, Map) when map_size(Map) == 0->
   Map;
jsn_with(Paths, Map) ->
   jsn:with(paths(Paths), Map).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Convert a binary path into its tuple-form, only when array-indices are used in the path.
%% This is required, because binary paths do not support array indices.
-spec paths(list(binary())) -> list(binary()|tuple()).
paths(Paths) when is_list(Paths) ->
   [path(Path) || Path <- Paths].


-spec path(binary()|tuple()) -> binary()|tuple().
path(Path) when is_tuple(Path) -> Path;
path(Path) when is_binary(Path) ->
   case ( catch ets:lookup(field_paths, Path)) of
      [{Path, Cached}] -> Cached;
      [] ->
         Ret = convert_path(Path),
         ets:insert(field_paths, {Path, Ret}), Ret;
      _ -> Ret = convert_path(Path),  Ret
end.

convert_path(Path) ->
   case binary:match(Path, [<<"[">>, ?DOT_ESCAPE]) of
      nomatch -> Path;
      _Match ->
         Split0 = binary:split(Path, [<<".">>], [global, trim_all]),
         Split = [binary:replace(BinPart, ?DOT_ESCAPE, <<".">>, [global]) || BinPart <- Split0],
         PathList =
            lists:foldl(
               fun(E, List) ->
                  List ++ extract_array_index(E)
               end,
               [],
               Split),
         list_to_tuple(PathList)
   end.

extract_array_index(Bin) ->
   case binary:split(Bin, [<<"[">>,<<"]">>], [global, trim_all]) of
      [Bin] = Out -> Out;
      [Part1, BinIndex] ->
         %% we need to add +1 to the array index, jsn array indices are 1-based
         [Part1, binary_to_integer(BinIndex)+1]
   end.

%% @doc whether the given path is a root path, ie: can be set with maps:put to fields or tags
-spec is_root_path(binary()) -> true|false.
is_root_path(Path) when is_binary(Path) ->
   case binary:match(Path, [<<".">>,<<"[">>]) of
      nomatch -> true;
      _ -> false
   end;
is_root_path({Path}) when is_binary(Path) ->
   true;
is_root_path(_) ->
   false.

%% @doc
%% set NewRoot as the new root for the fields entry in a data_point record.
%% If the Path NewRoot already exists in the fields map, the data_point record is left untouched.
%% @end
-spec set_root(#data_point{}|#data_batch{}, binary()|tuple()|undefined) -> #data_point{}.
set_root(Item, undefined) ->
   Item;
set_root(Item, <<>>) ->
   Item;
set_root(Batch = #data_batch{points = Points}, NewRoot) when is_binary(NewRoot) orelse is_tuple(NewRoot) ->
   NewPoints = [set_root(P, NewRoot) || P <- Points],
   Batch#data_batch{points = NewPoints};
set_root(Point = #data_point{}, NewRoot) when is_binary(NewRoot) ->
   case is_root_path(NewRoot) of
      true ->
         set_root_key(Point, NewRoot);
      false ->
         set_root_path(Point, NewRoot)
   end.

%% @doc
%% used to set a new root to the fields key, when NewRoot is not a path, but a single level
%% @end
set_root_key(Point = #data_point{fields = Fields}, NewRoot) ->
   case maps:is_key(NewRoot, Fields) of
      true -> Point;
      false -> Point#data_point{fields = #{NewRoot => Fields}}
   end.

%% @doc
%% used to set a new root to the fields key, when NewRoot is a deep path
set_root_path(Point = #data_point{fields = Fields}, NewRoot) ->
   Path = path(NewRoot),
   case jsn:get(Path, Fields) of
      undefined -> Point#data_point{fields = jsn:set(Path, #{}, Fields)};
      _Preset -> Point
   end.

%% convert every suitable value to a numeric type
%% with a path
to_num(P, Path) when is_record(P, data_point) ->
   Res = to_num(field(P, Path)),
   set_field(P, Path, Res).
%% or just the root map of a data_point
to_num(P = #data_point{fields = Fields}) ->
   P#data_point{fields = to_num(Fields)};
%% or directly on a map
to_num(Map) when is_map(Map) ->
   F = fun(_K, Val) -> faxe_util:to_num(Val) end,
   maps:map(F, Map);
to_num(In) -> In.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge funcs
%% @todo document !!
merge([H|_]=List) when is_map(H), is_list(List) ->
   mapz:deep_merge(merge_fun(), #{}, List).
merge(M1, M2) when is_map(M1), is_map(M2) ->
   merge([M1, M2]);
merge(M1, M2) when is_list(M1), is_list(M2) -> lists:merge(M1, M2);
merge(V1, V2) when is_number(V1), is_number(V2) -> V1 + V2;
merge(S1, S2) when is_binary(S1), is_binary(S2) -> string:concat(S1, S2);
merge(_, _) -> error(cannot_merge_datatypes).
merge_fun() ->
   fun
      (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
      (Prev, Val) when is_list(Prev), is_list(Val) -> lists:merge(Prev, Val);
      (_, Val) -> Val
   end.

%%%%%%%%%%%%%%%%%%%%%
%%get_schema(P = #data_point{ts = Ts, fields = Fields, tags = Tags}) ->
%%   translate(Fields).
%%
%%translate(Fields) when is_map(Fields) ->
%%   Fun = fun
%%            (K, V, Acc) when is_map(V) -> Acc#{K => ok};
%%            (K, V, Acc) when is_list(V) -> Acc#{K => ok};
%%            (K, V, Acc) when is_float(V) -> Acc#{K => float};
%%            (K, V, Acc) when is_integer(V) -> Acc#{K => int};
%%            (K, V, Acc) when is_binary(V) -> Acc#{K => string}
%%         end,
%%   maps:fold(Fun, #{}, Fields).
%%
%%translate(K, V, Acc) when is_map(V) -> Acc#{K => ok};
%%translate(K, V, Acc) when is_list(V) -> Acc#{K => ok};
%%translate(K, V, Acc) when is_float(V) -> Acc#{K => float};
%%translate(K, V, Acc) when is_integer(V) -> Acc#{K => int};
%%translate(K, V, Acc) when is_binary(V) -> Acc#{K => string}.
