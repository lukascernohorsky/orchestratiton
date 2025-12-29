-module(saai_util).
-export([gen_uuid/0, sha256/1]).

-spec gen_uuid() -> binary().
gen_uuid() ->
    <<A1:32, A2:16, A3:16, A4:16, A5:48>> = crypto:strong_rand_bytes(16),
    io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-8~3.16.0b-~12.16.0b", [A1, A2, A3 band 16#0fff, A4 band 16#3fff, A5])
    |> lists:flatten()
    |> list_to_binary().

sha256(Data) when is_binary(Data) ->
    <<Digest:256>> = crypto:hash(sha256, Data),
    list_to_binary(io_lib:format("~64.16.0b", [Digest])).
