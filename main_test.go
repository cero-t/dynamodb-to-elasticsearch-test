package main
import (
	"testing"
	"log"
)

func TestTimeConsuming(t *testing.T) {
	json := `{
    "Records": [
        {
            "eventID": "1234567890",
            "eventName": "INSERT",
            "eventVersion": "1.0",
            "eventSource": "aws:dynamodb",
            "awsRegion": "ap-northeast-1",
            "dynamodb": {
                "Keys": {
                    "key1": {
                        "S": "id1"
                    },
                    "key2": {
                        "S": "id2"
                    }
                },
                "NewImage": {
                    "nullvalue": {
                        "NULL": true
                    },
                    "str": {
                        "S": "aaa"
                    },
                    "bool": {
                        "BOOL": true
                    },
                    "strSet": {
                        "SS": [
                            "aaa",
                            "bbb"
                        ]
                    },
                    "numset": {
                        "NS": [
                            "222",
                            "111"
                        ]
                    },
                    "bin": {
                        "B": "0000"
                    },
                    "num": {
                        "N": "1234"
                    },
                    "key1": {
                        "S": "id1"
                    },
                    "key2": {
                        "S": "id2"
                    },
                    "list": {
                        "L": [
                            {
                                "S": "aaa"
                            },
                            {
                                "N": "111"
                            },
                            {
                                "BOOL": false
                            }
                        ]
                    },
                    "binset": {
                        "BS": [
                            "0000",
                            "1111"
                        ]
                    },
                    "map": {
                        "M": {
                            "numVal": {
                                "N": "123"
                            },
                            "strVal": {
                                "S": "aaa"
                            },
                            "boolVal": {
                                "BOOL": false
                            }
                        }
                    }
                },
                "SequenceNumber": "123",
                "SizeBytes": 120,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            },
            "eventSourceARN": "arn:aws:dynamodb:ap-northeast-1:1234567899:table/mytest/stream/2016-01-01T00:00:00.000"
        }
    ]
}`

	result := parse(&json)
	log.Println(*result)
}