# Changelog

### 1.3.8 (Next)

* [#131](https://github.com/rcongiu/Hive-JSON-Serde/issues/131): Added support for mapping json keys with periods - [@rcongiu](https://github.com/rcongiu).
* [#148](https://github.com/rcongiu/Hive-JSON-Serde/pull/148): Fix: for `String` type input, `JavaStringDateObjectInspector#getPrimitiveJavaObject` should not return `null` - [@wangxianbin1987](https://github.com/wangxianbin1987).
* [#135](https://github.com/rcongiu/Hive-JSON-Serde/pull/135), [#134](https://github.com/rcongiu/Hive-JSON-Serde/pull/134), [#132](https://github.com/rcongiu/Hive-JSON-Serde/pull/132), [#130](https://github.com/rcongiu/Hive-JSON-Serde/pull/130), [#129](https://github.com/rcongiu/Hive-JSON-Serde/pull/129): Fix Sonar warnings - [@georgekankava](https://github.com/georgekankava).
* [#128](https://github.com/rcongiu/Hive-JSON-Serde/pull/128): Added support for timestamps in milliseconds - [@kevinstumpf](https://github.com/kevinstumpf).
* [#102](https://github.com/rcongiu/Hive-JSON-Serde/pull/102): When empty string appears where a Hive Map (JSON Object) was expected, treat it as NULL, not -1 - [@mhandwerker](https://github.com/mhandwerker).
* [#12](https://github.com/rcongiu/Hive-JSON-Serde/issues/12): If a field is declared an array but a scalar is found, coerce it - [@rcongiu](https://github.com/rcongiu).

### 1.3.7 (2015/12/10)

* Added JSON UDF - [@rcongiu](https://github.com/rcongiu).
* Added support for DATE type (Hive 1.2.0 and higher) - [@rcongiu](https://github.com/rcongiu).

### 1.3.6 (2015/10/08)

* [#117](https://github.com/rcongiu/Hive-JSON-Serde/pull/117): Added support for HDP 2.3 - [@dajobe](https://github.com/dajobe).
* [#118](https://github.com/rcongiu/Hive-JSON-Serde/pull/118): Added support for String to Boolean conversion - [@rjainqb](https://github.com/rjainqb).
* [#116](https://github.com/rcongiu/Hive-JSON-Serde/issues/116): Updated docs - [@rcongiu](https://github.com/rcongiu).

### 1.3.5 (2015/08/30)

* Made CDH5 default - [@rcongiu](https://github.com/rcongiu).
* [#53](https://github.com/rcongiu/Hive-JSON-Serde/issues/53): Added `UNIONTYPE` support - [@rcongiu](https://github.com/rcongiu).
* [#112](https://github.com/rcongiu/Hive-JSON-Serde/issues/112): Handle empty array where an empty object should be - [@rcongiu](https://github.com/rcongiu).
* [#98](https://github.com/rcongiu/Hive-JSON-Serde/pull/98): Added missing getPrimitiveJavaObject implementations - [@y-lan](https://github.com/y-lan).

### 1.3 (2014/09/08)

* [#82](https://github.com/rcongiu/Hive-JSON-Serde/issues/82): Fixed parsing empty strings to `map(string, string)` - [@rcongiu](https://github.com/rcongiu).
* [#84](https://github.com/rcongiu/Hive-JSON-Serde/issues/84): Deep name mapping - [@rcongiu](https://github.com/rcongiu).
* [#83](https://github.com/rcongiu/Hive-JSON-Serde/pull/83): - Fix for `\a` or `\v` that Java does not recognize in strings - [@ptrstpp950](https://github.com/ptrstpp950).
* [#86](https://github.com/rcongiu/Hive-JSON-Serde/pull/86): - Fix `PrimitiveObjectInspector#getPrimitiveJavaObject(Object)` - [@andykram](https://github.com/andykram).

### 1.2 (2014/06/01)

* Refactored to multimodule for CDH5 compatibility - [@rcongiu](https://github.com/rcongiu).
* [#90](https://github.com/rcongiu/Hive-JSON-Serde/pull/90): Fix `get_json_object` on Json String unless one defines it as struct - [@moss](https://github.com/wmoss).
* [#68](https://github.com/rcongiu/Hive-JSON-Serde/pull/68): Custom primitive object inspectors in the Serde need to override `getPrimitiveJavaObject` - [@appanasatya](https://github.com/appanasatya).

### 1.1.9.2 (2014/02/25)

* Added support for array records - [@rcongiu](https://github.com/rcongiu).
* Refactored timestamp handling - [@rcongiu](https://github.com/rcongiu).
* [#50](https://github.com/rcongiu/Hive-JSON-Serde/issues/50): Fixed issue with `{ field = null }` - [@rcongiu](https://github.com/rcongiu).
* [#54](https://github.com/rcongiu/Hive-JSON-Serde/issues/54): Fixed handling of `null` in arrays - [@rcongiu](https://github.com/rcongiu).
* [#55](https://github.com/rcongiu/Hive-JSON-Serde/pull/55): Fixed wrong cast - [@Powerrr](https://github.com/Powerrr).
* [#52](https://github.com/rcongiu/Hive-JSON-Serde/pull/52): Fixed `tynyint/byte` type - [@Powerrr](https://github.com/Powerrr).

### 1.1.9.1 (2014/02/02)

* Fixed wrong type (`long`) in `JavaStringIntObjectInspector` - [@rcongiu](https://github.com/rcongiu).
* [#22](https://github.com/rcongiu/Hive-JSON-Serde/pull/22): Verify castability - [@elreydetodo](https://github.com/elreydetodo).

### 1.1.8 (2014/01/22)

* Rewritten handling of numbers, so their parsing from string is delayed - [@rcongiu](https://github.com/rcongiu).
* [#39](https://github.com/rcongiu/Hive-JSON-Serde/issues/39): Fixed `testTimestampDeSerializeNumericTimestampWithNanoseconds` in different timezones - [@rcongiu](https://github.com/rcongiu).
* [#45](https://github.com/rcongiu/Hive-JSON-Serde/issues/45): Fixed `String` cannot be cast to `Integer` - [@rcongiu](https://github.com/rcongiu).
* [#34](https://github.com/rcongiu/Hive-JSON-Serde/issues/34): Fixed `Integer` cannot be cast to `Long` - [@rcongiu](https://github.com/rcongiu).
* [#43](https://github.com/rcongiu/Hive-JSON-Serde/issues/43): Fixed escape characters handling - [@rcongiu](https://github.com/rcongiu).
* [#29](https://github.com/rcongiu/Hive-JSON-Serde/issues/29): Fixed double cast - [@rcongiu](https://github.com/rcongiu).
* [#26](https://github.com/rcongiu/Hive-JSON-Serde/issues/26): Fixed `Integer` cannot be cast to `Double` - [@rcongiu](https://github.com/rcongiu).
* [#13](https://github.com/rcongiu/Hive-JSON-Serde/issues/13): Support Hive `FLOAT` - [@rcongiu](https://github.com/rcongiu).
* [#40](https://github.com/rcongiu/Hive-JSON-Serde/pull/40): Fixed `JSONObject#equals` - [@leobispo](https://github.com/leobispo).

### 1.1.7 (2013/09/30)

* [#31](https://github.com/rcongiu/Hive-JSON-Serde/issues/31): Fixed static members not to be - [@rcongiu](https://github.com/rcongiu).
* [#25](https://github.com/rcongiu/Hive-JSON-Serde/issues/25): Added basic timestamp support in deserializer - [@rcongiu](https://github.com/rcongiu), [@guyrt](https://github.com/guyrt).

### 1.1.6 (2013/07/10)

* [#28](https://github.com/rcongiu/Hive-JSON-Serde/issues/28): Fixed error after `ALTER TABLE ADD COLUMNS` - [@rcongiu](https://github.com/rcongiu), [@brndnmtthws](https://github.com/brndnmtthws).

### 1.1.4 (2012/10/04)

* [#13](https://github.com/rcongiu/Hive-JSON-Serde/issues/13): Problem with floats - [@rcongiu](https://github.com/rcongiu), [@ChuckConnell](https://github.com/ChuckConnell).
* [#7](https://github.com/rcongiu/Hive-JSON-Serde/pull/7): Fix handling of `{ arrayProp: null }` when expecting `{ arrayProp:[] }` - [@peterdm](https://github.com/peterdm).

### 1.1.2 (2012/07/26)

* Fixed columns not mapped into JSON - [@rcongiu](https://github.com/rcongiu), [@pmohan6](https://github.com/pmohan6).

### 1.1.1 (2012/07/03)

* Fixed infinite loop in `MapAdapter` - [@rcongiu](https://github.com/rcongiu).

### 1.1 (2011/09/09)

* Fixed 'string' type to accept non-string data in JSON representation - [@rcongiu](https://github.com/rcongiu).

### 1.0 (2011/07/12)

* Initial public release - [@rcongiu](https://github.com/rcongiu).

