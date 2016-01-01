package main

import (
	"os"
	"github.com/bitly/go-simplejson"
	"log"
	"strconv"
)

func main() {
	for i, arg := range os.Args {
		log.Printf("args[%d] : %v\n", i, arg)
	}

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
                    "id": {
                        "S": "id1"
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
                    "id": {
                        "S": "id1"
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
                            }                        }
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

	parse(&json)
}

func parse(jsonStr *string) {
	js, _ := simplejson.NewJson([]byte(*jsonStr))
	records := js.Get("Records")
	size := len(records.MustArray())

	bulkRequest := make([]byte, 0, 1024)

	for i := 0; i < size; i++ {
		record := records.GetIndex(i)

		index := map[string]interface{}{}
		index["_index"] = "mytest"
		index["_type"] = "mytest"

		keys := record.GetPath("dynamodb", "Keys")
		for k1, _ := range keys.MustMap() {
			typedValue := keys.Get(k1)
			index["_id"] = parseValue(typedValue)
		}

		action := simplejson.New()
		eventName := record.Get("eventName").MustString()
		if eventName == "INSERT" {
			action.Set("create", index)
		} else if eventName == "MODIFY" {
			action.Set("update", index)
		} else if eventName == "REMOVE" {
			action.Set("delete", index)
		} else {
			log.Fatal("Unknown eventName: " + eventName)
		}
		json, _ := action.Encode()

		bulkRequest = append(bulkRequest, json...)
		bulkRequest = append(bulkRequest, "\n"...)

		if eventName == "INSERT" || eventName == "MODIFY" {
			doc := simplejson.New()
			image := record.Get("dynamodb").Get("NewImage")
			for k1, _ := range image.MustMap() {
				value := parseValue(image.Get(k1))
				doc.Set(k1, value)
			}

			json, _ = doc.Encode()
			bulkRequest = append(bulkRequest, json...)
			bulkRequest = append(bulkRequest, "\n"...)
		}
	}

	log.Println(string(bulkRequest))
}

func parseValue(typedJson *simplejson.Json) interface{} {
	for k, v := range typedJson.MustMap() {
		log.Println("parseValue: ", k, v)

		switch k {
		case "S", "B", "BOOL":
			return v
		case "SS":
			return typedJson.Get("SS").MustArray()
		case "BS":
			return typedJson.Get("BS").MustArray()
		case "N":
			value, _ := strconv.Atoi(typedJson.Get("N").MustString())
			return value
		case "NS":
			return parseNumArray(typedJson.Get("NS"))
		case "L":
			return parseList(typedJson.Get("L"))
		case "M":
			return parseMap(typedJson.Get("M"))
		case "NULL":
			return nil
		}
	}

	return nil
}

func parseNumArray(arrayJson *simplejson.Json) []int {
	size := len(arrayJson.MustArray())
	result := make([]int, size, size)

	for i := 0; i < size; i++ {
		result[i], _ = strconv.Atoi(arrayJson.GetIndex(i).MustString())
	}

	return result
}

func parseMap(mapJson *simplejson.Json) map[string]interface{} {
	result := map[string]interface{}{}

	mapValue := mapJson.MustMap()
	for k, _ := range mapValue {
		result[k] = parseValue(mapJson.Get(k))
	}

	return result
}

func parseList(listJson *simplejson.Json) []interface{} {
	size := len(listJson.MustArray())
	result := make([]interface{}, size, size)

	for i := 0; i < size; i++ {
		for k, _ := range listJson.GetIndex(i).MustMap() {
			result[i] = string(listJson.GetIndex(i).Get(k).)
		}
	}

	return result
}

