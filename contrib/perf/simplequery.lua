-- evaluate a simple query
-- TODO: Evaluate query filter

wrk.method = "POST"
-- TODO: Simplest possible query
wrk.body = [[
{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "INCLUDE_ALL_VERSIONS"
        ,   "EXPAND_CONTENT"
        ],
        "query": {
            "term": {
                "matchType": "1",
                "matchValue": "*",
                "propertyName": "message"
            }
        }
    }
}
]]
