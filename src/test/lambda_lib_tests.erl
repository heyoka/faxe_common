%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Mar 2022 16:01
%%%-------------------------------------------------------------------
-module(lambda_lib_tests).
-author("heyoka").

%% API
-include("faxe_common.hrl").

-ifdef(TEST).
-compile(nowarn_export_all).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").


json_list_data() ->
  <<"[
  {\"key\": \"cond_vac\", \"dataformat\": \"0001.1110\",
  \"stream\":\"23u23jhr2iohjrfo23\", \"topic\" : \"data/0x867/23u23jhr2iohjrfo23/0001.1110\"},
  {\"key\": \"cond_scale\", \"dataformat\": \"0001.1120\",
  \"stream\": \"23898hduh2diuihd\", \"topic\" : \"data/0x867/23898hduh2diuihd/0001.1120\"},
  {\"key\": \"cond_robot\", \"dataformat\": \"0001.1130\",
   \"stream\": \"le232343242342343\", \"topic\" : \"le232343242342343/0001.1130\"}\n]">>
.

mem_select_jarray_1_test() ->
  TheList = json_list_data(),
  Expected = <<"le232343242342343">>,
  ?assertEqual(Expected, faxe_lambda_lib:select_first(<<"stream">>, [{<<"dataformat">>, <<"0001.1130">>}], TheList)).

mem_select_jarray_2_test() ->
  TheList = json_list_data(),
  Expected = <<"0001.1110">>,
  ?assertEqual(Expected, faxe_lambda_lib:select_first(<<"dataformat">>, [{<<"key">>, <<"cond_vac">>}], TheList)).

mem_select_jarray_notfound_test() ->
  TheList = json_list_data(),
  Expected = [],
  Res = faxe_lambda_lib:select(<<"key">>, [{<<"cond_drive">>, <<"dataformat">>}], TheList),
  ?assertEqual(Expected, Res).

mem_select_jarray_all_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>,<<"cond_scale">>,<<"cond_robot">>],
  Res = faxe_lambda_lib:select(<<"key">>, TheList),
  ?assertEqual(Expected, Res).

mem_select_first_jarray_test() ->
  TheList = json_list_data(),
  Expected = <<"cond_vac">>,
  Res = faxe_lambda_lib:select_first(<<"key">>, TheList),
  ?assertEqual(Expected, Res).

mem_select_first_jarray_where_test() ->
  TheList = json_list_data(),
  Expected = <<"cond_vac">>,
  Res = faxe_lambda_lib:select_first(<<"key">>, [{<<"dataformat">>, <<"0001.1110">>}], TheList),
  ?assertEqual(Expected, Res).

mem_select_first_key_undefined_test() ->
  TheList = json_list_data(),
  ?assertError("faxe_lambda_lib:select_first returned undefined",
    faxe_lambda_lib:select_first(<<"notkey">>, [{<<"dataformat">>, <<"0001.1110">>}], TheList)).

mem_select_first_any_jarray_all_where_test() ->
  TheList = json_list_data(),
  Expected = <<"cond_vac">>,
  Res = faxe_lambda_lib:select_any(<<"key">>,
    [{<<"strem">>, <<"le23234324234werw343">>}, {<<"dataformat">>, <<"0001.1110">>}],
    TheList),
  ?assertEqual(Expected, Res).

mem_select_first_any_jarray_undefined_test() ->
  TheList = json_list_data(),
  Expected = undefined,
  Res = faxe_lambda_lib:select_any(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {<<"dataformat">>, <<"0001.1110-t">>}],
    TheList),
  ?assertEqual(Expected, Res).

mem_select_first_any_jarray_undefined_default_test() ->
  TheList = json_list_data(),
  Expected = 133,
  Res = faxe_lambda_lib:select_any(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {<<"dataformat">>, <<"0001.1110-t">>}],
    TheList, 133),
  ?assertEqual(Expected, Res).

mem_select_any_jarray_where_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>],
  Res = faxe_lambda_lib:select_all(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {<<"dataformat">>, <<"0001.1110">>}],
    TheList),
  ?assertEqual(Expected, Res).

mem_select_any_jarray_where_multiple_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_robot">>, <<"cond_vac">>],
  Res = faxe_lambda_lib:select_all(<<"key">>,
    [{<<"stream">>, <<"le232343242342343">>}, {<<"dataformat">>, <<"0001.1110">>}],
    TheList),
  ?assertEqual(Expected, Res).

mem_select_any_jarray_undefined_test() ->
  TheList = json_list_data(),
  Expected = [],
  Res = faxe_lambda_lib:select_all(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {<<"dataformat">>, <<"0001.1110-t">>}],
    TheList),
  ?assertEqual(Expected, Res).

mem_select_any_jarray_undefined_default_test() ->
  TheList = json_list_data(),
  Expected = 266,
  Res = faxe_lambda_lib:select_all(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {<<"dataformat">>, <<"0001.1110-t">>}],
    TheList, 266),
  ?assertEqual(Expected, Res).


regex_select_jarray_1_test() ->
  TheList = json_list_data(),
  Expected = 266,
  Res = faxe_lambda_lib:select_all(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {regex, <<"dataformat">>, <<"-t">>}],
    TheList, 266),
  ?assertEqual(Expected, Res).

regex_select_jarray_2_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>,<<"cond_scale">>,<<"cond_robot">>],
  Res = faxe_lambda_lib:select_all(<<"key">>,
    [{<<"stream">>, <<"le23234324234werw343">>}, {regex, <<"dataformat">>, <<"0001">>}],
    TheList, 266),
  ?assertEqual(Expected, Res).

regex_select_jarray_3_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>],
  Res = faxe_lambda_lib:select_all(<<"key">>, [{<<"regex">>, <<"dataformat">>, <<"^[0-9]{4}\.1110$">>}],
    TheList, 266),
  ?assertEqual(Expected, Res).

regex_select_jarray_4_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>],
  Res = faxe_lambda_lib:select_all(<<"key">>, [{regex, <<"dataformat">>, <<"1110$">>}],
    TheList, 266),
  ?assertEqual(Expected, Res).

regex_select_jarray_5_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>,<<"cond_scale">>,<<"cond_robot">>],
  Res = faxe_lambda_lib:select_all(<<"key">>, [{regex, <<"dataformat">>, <<"11[0-9]{2}$">>}],
    TheList, []),
  ?assertEqual(Expected, Res).

regex_select_jarray_6_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>],
  Res = faxe_lambda_lib:select(<<"key">>, [{regex, <<"dataformat">>, <<"11[0-9]{2}$">>}, {regex, <<"key">>, <<"_vac$">>}],
    TheList, []),
  ?assertEqual(Expected, Res).

regex_select_jarray_7_test() ->
  TheList = json_list_data(),
  Expected = [<<"0001.1110">>,<<"0001.1120">>,<<"0001.1130">>],
  Res = faxe_lambda_lib:select(<<"dataformat">>, [{regex, <<"dataformat">>, <<"11[0-9]{2}$">>}, {regex, <<"key">>, <<"^cond">>}],
    TheList, []),
  ?assertEqual(Expected, Res).

regex_select_jarray_8_test() ->
  TheList = json_list_data(),
  Expected = [<<"0001.1120">>],
  Res = faxe_lambda_lib:select(<<"dataformat">>, [{<<"stream">>, <<"23898hduh2diuihd">>}, {regex, <<"key">>, <<"^cond">>}],
    TheList, []),
  ?assertEqual(Expected, Res).

regex_select_first_jarray_1_test() ->
  TheList = json_list_data(),
  Expected = <<"cond_vac">>,
  Res = faxe_lambda_lib:select_first(<<"key">>, [{regex, <<"dataformat">>, <<"11[0-9]{2}$">>}],
    TheList, <<>>),
  ?assertEqual(Expected, Res).

regex_select_first_jarray_2_test() ->
  TheList = json_list_data(),
  Expected = <<"0001.1120">>,
  Res = faxe_lambda_lib:select_first(<<"dataformat">>,
    [{<<"stream">>, <<"23898hduh2diuihd">>}, {<<"regex">>, <<"key">>, <<"^cond">>}],
    TheList, []),
  ?assertEqual(Expected, Res).

regex_select_first_jarray_default_test() ->
  TheList = json_list_data(),
  Expected = [],
  Res = faxe_lambda_lib:select_first(<<"key">>, [{regex, <<"dataformat">>, <<"19[0-9]{2}$">>}],
    TheList, []),
  ?assertEqual(Expected, Res).

map_get_bin_test() ->
  M = <<"{\"key\": \"value\"}">>,
  Expected = <<"value">>,
  ?assertEqual(Expected, faxe_lambda_lib:map_get(<<"key">>, M)).

map_get_bin_default_test() ->
  M = <<"{\"key\": \"value\"}">>,
  ?assertEqual(<<"undefined">>, faxe_lambda_lib:map_get(<<"key1">>, M)).

map_get_bin_default_given_test() ->
  M = <<"{\"key\": \"value\"}">>,
  ?assertEqual(0, faxe_lambda_lib:map_get(<<"key1">>, M, 0)).

map_get_bin_default2_test() ->
  M = <<"{\"key\": \"value\"}">>,
  ?assertEqual(<<"default">>, faxe_lambda_lib:map_get(<<"key1">>, M, <<"default">>)).


is_duration_succeeds_test() ->
  ?assertEqual(true, faxe_lambda_lib:is_duration(<<"234ms">>)).

is_duration_succeeds_2_test() ->
  ?assertEqual(true, faxe_lambda_lib:is_duration(<<"0h">>)).

is_duration_fails_1_test() ->
  ?assertEqual(false, faxe_lambda_lib:is_duration(<<"234mms">>)).

is_duration_fails_2_test() ->
  ?assertEqual(false, faxe_lambda_lib:is_duration(314.2566)).

is_duration_fails_3_test() ->
  ?assertEqual(false, faxe_lambda_lib:is_duration(<<"314.2566s">>)).

-endif.
