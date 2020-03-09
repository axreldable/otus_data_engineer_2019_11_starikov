#!/bin/bash

HEADER="Content-Type: application/json"
DATA=$( cat << EOF
{
    "settings": {
        "number_of_shards": 1
    },
    "mappings": {
        "wikichange_short": {
            "properties": {
                "CREATEDAT": {
                    "type": "date"
                },
                "WIKIPAGE": {
                    "type": "keyword",
                    "index": "no"
                },
                "USERNAME": {
                    "type": "keyword"
                },
                "DIFFURL": {
                    "type": "text",
                    "index": "no"
                },
                "CHANNEL": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                }
            }
        }
    }
}

EOF);

curl -XPUT -H "${HEADER}" --data "${DATA}" 'http://localhost:9200/wikilang?pretty'
echo