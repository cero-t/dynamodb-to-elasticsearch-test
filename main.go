package main

import (
	"github.com/bitly/go-simplejson"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"reflect"
)

const (
	host = "localhost"
	port = 9200
)

func main() {
	for i, arg := range os.Args {
		log.Printf("args[%d] : %v\n", i, arg)
	}

	result := parse(&os.Args[1])
	body := toElasticsearch(result)
	log.Println(body)
}

func toElasticsearch(jsonStr *string) *[]byte {
	url := "http://" + host + ":" + strconv.Itoa(port) + "/_bulk"
	resp, err := http.Post(url, "text/json", strings.NewReader(*jsonStr))
	if err != nil {
		log.Println("Bulk request error", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	return &body
}

func parse(jsonStr *string) *string {
	js, _ := simplejson.NewJson([]byte(*jsonStr))
	records := js.Get("Records")
	size := len(records.MustArray())

	bulkRequest := make([]byte, 0, 1024)

	for i := 0; i < size; i++ {
		record := records.GetIndex(i)

		index := map[string]interface{}{}
		index["_index"] = "mytest"
		index["_type"] = "mytest"
		index["_id"] = parseKeys(record.GetPath("dynamodb", "Keys"))

		action := simplejson.New()
		eventName := record.Get("eventName").MustString()
		if eventName == "INSERT" {
			action.Set("create", index)
		} else if eventName == "MODIFY" {
			action.Set("update", index)
		} else if eventName == "REMOVE" {
			action.Set("delete", index)
		} else {
			log.Println("Unknown eventName", eventName)
			continue
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

	result := string(bulkRequest)
	return &result
}

func parseValue(typedJson *simplejson.Json) interface{} {
	for k, v := range typedJson.MustMap() {
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

func parseKeys(keysJson *simplejson.Json) string {
	keys := make([]string, 0, 2)
	for k, _ := range keysJson.MustMap() {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	content := make([]byte, 0, 64)
	for _, k1 := range keys {
		for k2, _ := range keysJson.Get(k1).MustMap() {
			if len(content) > 0 {
				content = append(content, ","...)
			}
			content = append(content, k1...)
			content = append(content, ":"...)
			content = append(content, keysJson.GetPath(k1, k2).MustString()...)
		}
	}

	return string(content)
}

func parseNumArray(arrayJson *simplejson.Json) *[]int {
	size := len(arrayJson.MustArray())
	result := make([]int, size, size)

	for i := 0; i < size; i++ {
		result[i], _ = strconv.Atoi(arrayJson.GetIndex(i).MustString())
	}

	return &result
}

func parseMap(mapJson *simplejson.Json) *map[string]interface{} {
	result := map[string]interface{}{}

	mapValue := mapJson.MustMap()
	for k, _ := range mapValue {
		result[k] = parseValue(mapJson.Get(k))
	}

	return &result
}

func parseList(listJson *simplejson.Json) *[]interface{} {
	size := len(listJson.MustArray())
	result := make([]interface{}, size, size)

	for i := 0; i < size; i++ {
		for _, v := range listJson.GetIndex(i).MustMap() {
			result[i] = *forceToString(&v)
		}
	}

	return &result
}

func forceToString(valRef *interface{}) *string {
	val := *valRef
	switch v := val.(type) {
	case string:
		return &v
	case bool:
		str := strconv.FormatBool(v)
		return &str
	}

	log.Println("Unknown type", reflect.TypeOf(val))
	str := "UNKNOWN"
	return &str
}
