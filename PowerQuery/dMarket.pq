let
    sql = "SELECT * FROM ""dMarket""",
    Source = Odbc.Query(ODBC_String, sql),
    Sort_ = Table.Sort(Source, {{"Market", Order.Ascending}}),
    // Some of the Media Monitors market labels are not
    //      readily caught by the Google Maps Search API
    GeocodeSearchVals = #table(type table [orig = text, repl = text],
                               {{"Mobile", "Mobile AL"},
                                {"Pittsburgh", "Pittsburgh PA"},
                                {"Riverside-San Bernardino", "Riverside CA"},
                                {"Middlesex", "Middlesex NJ"}}
    ),
    JoinReplace = Table.Join(Sort_, "Market",
                             GeocodeSearchVals, "orig",
                             JoinKind.LeftOuter),
    FixMarketName = Table.RemoveColumns(
        Table.ReplaceValue(JoinReplace,each [Market],
                           each if [repl] is null then [Market] else [repl],
                           Replacer.ReplaceValue,
                           {"Market"}),
        {"orig", "repl"}),
    GeocodeMarkets = Table.AddColumn(FixMarketName, "LatLon",
                                     each Geocode([Market])),
    FlattenCoords = Table.ExpandTableColumn(GeocodeMarkets, "LatLon",
                                            {"lat", "lng", "State"},
                                            {"lat", "lng", "State"}),
    DTypes = Table.TransformColumnTypes(
        FlattenCoords,
        {{"lat", type number}, {"lng", type number}}),
    RoundCoords = Table.TransformColumns(
        DTypes,
        {{"lat", each Number.Round(_, 4), type number},
         {"lng", each Number.Round(_, 4), type number}})
in
    RoundCoords