# DynamoDB streams to Elasticsearch

## What's this?

AWS Lambda scripts from DynamoDB streams to Elasticsearch written in Go.


## Type mapping

|DynamoDB data type  |Elasticsearch data type|Note |
| ------------------ | --------------------- | --- |
|String (S)          |string                 ||
|String Set (SS)     |string (multiple)      ||
|Number (N)          |number                 ||
|Number Set (NS)     |number (multiple)      ||
|Binary (B)          |string                 |`Binary` type cannot be mapped to proper type in current version, should be mapped to `binary` type.|
|Binary Set (BS)     |string (multiple)      |`Binary` type cannot be mapped to proper type in current version.|
|Boolean (BOOL)      |boolean                ||
|List (L)            |string (multiple)      |In DynamoDB `list` type may contain multiple types but Elasticsearch may not. Thus any type in `list` should be regarded as `string`.|
|Map (M)             |object/nested          |{"key": {"key1":"value1"}, {"key2":"value2"}}<br>is mapped to<br>{"key.key1":"value1","key.key2":"value2"}|
|Null (NULL)         |(unknown)              |`Null` field cannot be mapped any type.|
