This area contains all SQL queries used by DBS code. The queries use GoLang
template language and in addition follow this convention. For bind parameters
please use `:ParamName` syntax. It will be converted to `?` and `ParamName`
in a code where we can pass appropriate value to `ParamName`.
