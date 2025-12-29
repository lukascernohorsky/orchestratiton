-module(saai_time).
-export([deadline/1]).

deadline(TimeBudgetSeconds) when is_integer(TimeBudgetSeconds) ->
    {{Y,Mo,D},{H,Mi,S}} = calendar:universal_time(),
    Seconds = calendar:datetime_to_gregorian_seconds({{Y,Mo,D},{H,Mi,S}}) + TimeBudgetSeconds,
    calendar:gregorian_seconds_to_datetime(Seconds);
deadline(_Other) -> undefined.
