#! /bin/sh
#
#  cURL commands:


prompt () {
  echo ""
  echo ""
  echo $1
  echo "[Press Any Key]"
  read
}

####

prompt "List the example services"
curl http://127.0.0.1:8000/core/examples


prompt 'Query the set of documents with a field called "name" with value "Amanda":'
curl -X POST -H "Content-Type: application/json" -d '{
    "querySpec": {
        "query": {
            "term": {
                "matchValue": "Amanda",
                "propertyName": "name"
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Directly query the set of documents with a field called "name" with value "Amanda":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "query": {
            "term": {
                "matchValue": "Amanda",
                "propertyName": "name"
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Directly query the set of documents with a field called "name" with value "Amanda", with document contents:'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "term": {
                "matchValue": "Amanda",
                "propertyName": "name"
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Wildcard query for the set of documents whose name contains "man":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "term": {
                "matchType": "WILDCARD",
                "matchValue": "*man*",
                "propertyName": "name"
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query the set of documents whose "counter" field is between 1-10 inclusive:'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "term": {
                "propertyName": "counter",
                "range": {
                    "type": "LONG",
                    "min": 1.0,
                    "max": 10.0,
                    "isMinInclusive": "true",
                    "isMaxInclusive": "true"
                }
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query the set of documents containing the key "key1":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "term": {
                "matchValue": "key1",
                "propertyName": "keyValues"
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query the set of documents containing the value "value1":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "term": {
                "matchValue": "value1",
                "propertyName": "keyValues"
            }
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query the set of documents containing the key "key1" and the value "value1":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "booleanClauses": [
                {
                    "occurance": "MUST_OCCUR",
                    "term": {
                        "matchValue": "key1",
                        "propertyName": "keyValues"
                    }
                },
                {
                    "occurance": "MUST_OCCUR",
                    "term": {
                        "matchValue": "value1",
                        "propertyName": "keyValues"
                    }
                }
            ]
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query the set of documents containing the key "key1" *or* the value "value1":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "booleanClauses": [
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "key1",
                        "propertyName": "keyValues"
                    }
                },
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "value1",
                        "propertyName": "keyValues"
                    }
                }
            ]
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Paginated query for the set of documents containing the key "key1" or the value "value1":'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "booleanClauses": [
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "key1",
                        "propertyName": "keyValues"
                    }
                },
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "value1",
                        "propertyName": "keyValues"
                    }
                }
            ]
        },
        "resultLimit": 2
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query for the set of documents containing the key "key1" or the value "value1", sorted by name:'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "booleanClauses": [
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "key1",
                        "propertyName": "keyValues"
                    }
                },
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "value1",
                        "propertyName": "keyValues"
                    }
                }
            ]
        },
        "sortTerm": {
            "propertyName": "name",
            "propertyType": "STRING"
        }
    }
}' http://127.0.0.1:8000/core/query-tasks


prompt 'Query for the set of documents containing the key "key1" or the value "value1", sorted by descended sorted counter value:'
curl -X POST -H "Content-Type: application/json" -d '{
    "taskInfo": {
        "isDirect": true
    },
    "querySpec": {
        "options": [
            "EXPAND_CONTENT"
        ],
        "query": {
            "booleanClauses": [
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "key1",
                        "propertyName": "keyValues"
                    }
                },
                {
                    "occurance": "SHOULD_OCCUR",
                    "term": {
                        "matchValue": "value1",
                        "propertyName": "keyValues"
                    }
                }
            ]
        },
        "sortTerm": {
            "propertyName": "sortedCounter",
            "propertyType": "LONG"
        },
        "sortOrder": "DESC"
    }
}' http://127.0.0.1:8000/core/query-tasks

