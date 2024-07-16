# Snowflake To BigQuery Data Transfer Supported Datatype Guide [SFBQDT]

* For SFBQDT introduction, please refer to [/README.md](../README.md)
* For SFBQDT detailed documentation, please refer to [/DOCUMENTATION.md](DOCUMENTATION.md)
* For SFBQDT supported APIs details, please refer to [/REST_API.md](REST_API.md)

## 1. Supported Data Types

Presently, SFBQDT supports below-mentioned data type for Parquet and CSV format,means user can export data in either of CSV or Parquet format.

| Snowflake Data Type | PARQUET | CSV |
|----------------------|---------|-----|
| NUMBER               | Yes     | Yes |
| DECIMAL              | Yes     | Yes |
| INT                  | Yes     | Yes |
| INTEGER              | Yes     | Yes |
| BIGINT               | Yes     | Yes |
| SMALLINT             | Yes     | Yes |
| TINYINT              | Yes     | Yes |
| BYTEINT              | Yes     | Yes |
| FLOAT                | Yes     | Yes |
| FLOAT4               | Yes     | Yes |
| FLOAT8               | Yes     | Yes |
| DOUBLE               | Yes     | Yes |
| REAL                 | Yes     | Yes |
| VARCHAR              | Yes     | Yes |
| CHAR                 | Yes     | Yes |
| CHARACTER            | Yes     | Yes |
| STRING               | Yes     | Yes |
| TEXT                 | Yes     | Yes |
| BINARY               | Yes     | Yes |
| VARBINARY            | Yes     | Yes |
| DATE                 | Yes     | Yes |
| DATETIME             | Yes     | Yes |
| TIME                 | Yes     | Yes |
| TIMESTAMP            | Yes     | Yes |
| TIMESTAMP_NTZ        | Yes     | Yes |

## 2. Supported Data Types With Fix

Presently, SFBQDT does not provide support for the following datatype in both CSV and Parquet formats out of the box. However, 
with some adjustments, it can function for a particular format

| Snowflake Data Type | PARQUET                                                                                                                                                                                                                                                                 | CSV                                             |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------|
| TIMESTAMP_NTZ       | No                                                                                                                                                                                                                                                                      | Yes, when casted to String format using TO_CHAR |
| TIMESTAMP_TZ        | No                                                                                                                                                                                                                                                                      | Yes, when casted to String format using TO_CHAR |
| GEOGRAPHY           | Yes                                                                                                                                                                                                                                                                     | No                                              |
|                     | Translation service does not support the GEOGRAPHY data type,Therefore, users must create a table manually beforehand and refrain from relying on the connector to create it for them. The connector can still retrieve the data and load it into the table as needed.  |                                                 |

## 3. Supported Semi Structured data types with fix

Parquet unload operations only support string data types for lists/arrays. Additionally, unloading VARIANT data types in Parquet is not possible; the data will be converted into a simple string format instead..
VARIANT columns are converted into simple JSON strings rather than LIST values, even if the column values are cast to arrays (using the TO_ARRAY function).  It's a known limitation mentioned in public [documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-location) as well.

| Snowflake Data Type | PARQUET Support                                                                                                                                                                                                                                                                                                                                                                                                       | CSV  Support |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
|VARIANT             | Yes                                                                                                                                                                                                                                                                                                                                                                                                                   | No           |
|                     | The translation service converts the VARIANT data type to JSON. According to Snowflake documentation, VARIANT columns are transformed into basic JSON strings instead of LIST values. This constraint poses a challenge during end-to-end data migration using a tool, leading to errors. To address this issue, you can preemptively create a table with the column type set to String before transferring the data  |
| OBJECT              | Yes                                                                                                                                                                                                                                                                                                                                                                                                                   | No           |     
|                     | Same reasoning  as above(variant) datatype **Note:** Snowflake Limitation                                                                                                                                                                                                                                                                                                                                             |              |
| ARRAY               | Yes                                                                                                                                                                                                                                                                                                                                                                                                                   | No           |
|                     | Same reasoning as above(variant) datatype **Note:** Snowflake Limitation                                                                                                                                                                                                                                                                                                                                              |              |

# Disclaimer
This is not an officially supported Google product.