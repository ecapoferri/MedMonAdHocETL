(search_string_raw as text) => let
    url_params = Uri.BuildQueryString([
        key = APIKey, // ParameterQuery
        address = search_string_raw
        ]),
    url = "https://maps.googleapis.com/maps/api/geocode/json"
          & "?" & url_params,
    Source = Json.Document(Web.Contents(url)),
    JSON_Layer0 = Table.FromRecords({Source}),
    JSON_Layer1 = Table.ExpandListColumn(JSON_Layer0, "results"),
    TopResult = Table.FirstN(JSON_Layer1,1),
    Expand_results = Table.ExpandRecordColumn(
        TopResult, "results",
        {"address_components", "geometry"},
        {"address_components", "geometry"}),
    Expanded_address_components_list = Table.ExpandListColumn(Expand_results,
                                                         "address_components"),
    Expanded_address_components_records = Table.ExpandRecordColumn(
        Expanded_address_components_list,
        "address_components",
        {"short_name", "types"},
        {"State", "address_components.types"}),
    Expanded_address_components_types_list = Table.ExpandListColumn(
        Expanded_address_components_records,
        "address_components.types"),
    StateJSON = Table.SelectRows(
        Expanded_address_components_types_list,
        each ([address_components.types] = "administrative_area_level_1")),
    SELECT = Table.SelectColumns(StateJSON,{"State", "geometry", "status"}),
    Expand_geometry = Table.ExpandRecordColumn(SELECT, "geometry",
                                               {"location"}, {"location"}),
    Expand_location = Table.ExpandRecordColumn(Expand_geometry, "location",
                                               {"lat", "lng"}, {"lat", "lng"}),
    DTypes = Table.TransformColumnTypes(Expand_location,
                                        {
                                            {"lat", type number},
                                            {"lng", type number},
                                            {"State", type text}
                                        })
in
    DTypes