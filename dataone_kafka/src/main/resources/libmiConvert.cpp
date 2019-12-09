#include "libmiConvert.h"
#include "nspr.h"
#include <string.h>

using namespace MICONVERT;

#ifdef WIN32
#define strcasecmp stricmp
#define strncasecmp strnicmp
#endif

#define DBTYPE_ORACLE	1
#define DBTYPE_MYSQL	2
#define DBTYPE_MSSQL	3

static void MiOracleToOracle(TypeInfo *src, TypeInfo *dst);
static void MiOracleToMysql(TypeInfo *src, TypeInfo *dst);
static void MiOracleToMssql(TypeInfo *src, TypeInfo *dst);

static void MiMysqlToMysql(TypeInfo *src, TypeInfo *dst);
static void MiMysqlToOracle(TypeInfo *src, TypeInfo *dst);
static void MiMysqlToMssql(TypeInfo *src, TypeInfo *dst);

static void MiMssqlToMssql(TypeInfo *src, TypeInfo *dst);
static void MiMssqlToOracle(TypeInfo *src, TypeInfo *dst);
static void MiMssqlToMysql(TypeInfo *src, TypeInfo *dst);

void MICONVERT::MiConvert(int srcType, int dstType, TypeInfo *src, TypeInfo *dst)
{
	switch(srcType){
	case DBTYPE_ORACLE:
		{
			switch(dstType){
			case DBTYPE_ORACLE:
				MiOracleToOracle(src, dst);
				break;
			case DBTYPE_MYSQL:
				MiOracleToMysql(src, dst);
				break;
			case DBTYPE_MSSQL:
				MiOracleToMssql(src, dst);
				break;
			default:
				break;
			}
		}
		break;
	case DBTYPE_MYSQL:
		{
			switch(dstType){
			case DBTYPE_ORACLE:
				MiMysqlToOracle(src, dst);
				break;
			case DBTYPE_MYSQL:
				MiMysqlToMysql(src, dst);
				break;
			case DBTYPE_MSSQL:
				MiMysqlToMssql(src, dst);
				break;
			default:
				break;
			}
		}
		break;
	case DBTYPE_MSSQL:
		{
			switch(dstType){
			case DBTYPE_ORACLE:
				MiMssqlToOracle(src, dst);
				break;
			case DBTYPE_MYSQL:
				MiMssqlToMysql(src, dst);
				break;
			case DBTYPE_MSSQL:
				MiMssqlToMssql(src, dst);
				break;
			default:
				break;
			}
		}
		break;
	default:
		break;
	}
}


static void MiOracleToOracle(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(src->szDataType, "XMLTYPE") == 0){
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "VARCHAR2") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NVARCHAR2") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NUMBER") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
	}else if(strcasecmp(src->szDataType, "LONG") == 0){
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
	}else if(strcasecmp(src->szDataType, "BINARY_FLOAT") == 0){
	}else if(strcasecmp(src->szDataType, "BINARY_DOUBLE") == 0){
	}else if(strcasecmp(src->szDataType, "TIMESTAMP") == 0){
	}else if(strcasecmp(src->szDataType, "CLOB") == 0){
	}else if(strcasecmp(src->szDataType, "NCLOB") == 0){
	}else if(strcasecmp(src->szDataType, "BLOB") == 0){
	}else if(strcasecmp(src->szDataType, "RAW") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "LONG RAW") == 0){
	}else if(strcasecmp(src->szDataType, "INTEGER") == 0){
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
	}else if(strcasecmp(src->szDataType, "BFILE") == 0){
	}

	if(dst->type_owner[0] != '\0'){
		memset(dst->szDataType, 0, 128);
		PR_snprintf(dst->szDataType, 128, "%s.%s", src->type_owner, src->szDataType);
	}
}

static void MiOracleToMysql(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(src->szDataType, "XMLTYPE") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "VARCHAR2") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NVARCHAR2") == 0){
		strcpy(dst->szDataType, "NATIONAL VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCHAR") == 0){
		strcpy(dst->szDataType, "NATIONAL VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NUMBER") == 0){
		strcpy(dst->szDataType, "DECIMAL");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
		strcpy(dst->szDataType, "DOUBLE");
	}else if(strcasecmp(src->szDataType, "LONG") == 0){
		strcpy(dst->szDataType, "LONGTEXT");
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "BINARY_FLOAT") == 0){
		strcpy(dst->szDataType, "DECIMAL");
		strcpy(dst->prec, "65");
		strcpy(dst->scale, "8");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "BINARY_DOUBLE") == 0){
		strcpy(dst->szDataType, "DOUBLE");
	}else if(strncasecmp(src->szDataType, "TIMESTAMP", strlen("TIMESTAMP")) == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "CLOB") == 0){
		strcpy(dst->szDataType, "LONGTEXT");
	}else if(strcasecmp(src->szDataType, "NCLOB") == 0){
		strcpy(dst->szDataType, "LONGTEXT");
	}else if(strcasecmp(src->szDataType, "BLOB") == 0){
		strcpy(dst->szDataType, "LONGBLOB");
	}else if(strcasecmp(src->szDataType, "RAW") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "2000");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "LONG RAW") == 0){
		strcpy(dst->szDataType, "LONGBLOB");
	}else if(strcasecmp(src->szDataType, "INTEGER") == 0){		
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
	}else if(strcasecmp(src->szDataType, "BFILE") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "2000");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "UROWID") == 0){
		strcpy(dst->szDataType, "CHAR");
		strcpy(dst->length, "18");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "ROWID") == 0){
		strcpy(dst->szDataType, "CHAR");
		strcpy(dst->length, "18");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "INTERVAL DAY", strlen("INTERVAL DAY")) == 0){
		strcpy(dst->szDataType, "TINYTEXT");
	}else if(strncasecmp(src->szDataType, "INTERVAL YEAR", strlen("INTERVAL YEAR")) == 0){
		strcpy(dst->szDataType, "TINYTEXT");
	}
}

static void MiOracleToMssql(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(src->szDataType, "XMLTYPE") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "BFILE") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "BLOB") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CLOB") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCLOB") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
	}else if(strcasecmp(src->szDataType, "BINARY_FLOAT") == 0){
		strcpy(dst->szDataType, "DECIMAL");
		strcpy(dst->prec, "38");
		strcpy(dst->scale, "8");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "BINARY_DOUBLE") == 0){
		strcpy(dst->szDataType, "DECIMAL");
		strcpy(dst->prec, "38");
		strcpy(dst->scale, "8");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "INTERVAL") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "LONG") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "RAW") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		//strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "LONG RAW") == 0){
		strcpy(dst->szDataType, "IMAGE");
	}else if(strcasecmp(src->szDataType, "NCLOB") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NVARCHAR2") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
		strcpy(dst->szDataType, "FLOAT");
	}else if(strncasecmp(src->szDataType, "TIMESTAMP", strlen("TIMESTAMP")) == 0){
		//strcpy(dst->szDataType, "DATETIME");
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "100");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "INTERVAL DAY", strlen("INTERVAL DAY")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "30");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "INTERVAL YEAR", strlen("INTERVAL YEAR")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "30");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "UROWID") == 0){
		strcpy(dst->szDataType, "CHAR");
		strcpy(dst->length, "18");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "ROWID") == 0){
		strcpy(dst->szDataType, "CHAR");
		strcpy(dst->length, "18");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "VARCHAR2") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NUMBER") == 0){
		strcpy(dst->szDataType, "NUMERIC");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "INTEGER") == 0){
		strcpy(dst->szDataType, "INT");
	}else if(strcasecmp(src->szDataType, "BFILE") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}
}

static void MiMysqlToMysql(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
	}else if(strcasecmp(src->szDataType, "BIGINT") == 0){
	}else if(strcasecmp(src->szDataType, "INT") == 0){
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "BIT") == 0){
	}else if(strcasecmp(src->szDataType, "BINARY") == 0){
	}else if(strcasecmp(src->szDataType, "VARBINARY") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DATETIME") == 0){
	}else if(strcasecmp(src->szDataType, "BLOB") == 0){
	}else if(strcasecmp(src->szDataType, "DOUBLE") == 0){
	}else if(strcasecmp(src->szDataType, "INT") == 0){
	}else if(strcasecmp(src->szDataType, "INTEGER") == 0){
	}else if(strcasecmp(src->szDataType, "ENUM") == 0){
	}else if(strcasecmp(src->szDataType, "LONGBLOB") == 0){
	}else if(strcasecmp(src->szDataType, "LONGTEXT") == 0){
	}else if(strcasecmp(src->szDataType, "MEDIUMBLOB") == 0){
	}else if(strcasecmp(src->szDataType, "MEDIUMINT") == 0){
	}else if(strcasecmp(src->szDataType, "MEDIUMTEXT") == 0){
	}else if(strcasecmp(src->szDataType, "NUMERIC") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "SET") == 0){
	}else if(strcasecmp(src->szDataType, "SMALLINT") == 0){
	}else if(strcasecmp(src->szDataType, "TEXT") == 0){
	}else if(strcasecmp(src->szDataType, "TIME") == 0){
	}else if(strcasecmp(src->szDataType, "TIMESTAMP") == 0){
	}else if(strcasecmp(src->szDataType, "TINYBLOB") == 0){
	}else if(strcasecmp(src->szDataType, "TINYINT") == 0){
	}else if(strcasecmp(src->szDataType, "TINYTEXT") == 0){
	}else if(strcasecmp(src->szDataType, "VARCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "YEAR") == 0){
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
	}
}

static void MiMysqlToOracle(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strncasecmp(src->szDataType, "CHAR", strlen("CHAR")) == 0){
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "BOOL", strlen("BOOL")) == 0){
		strcpy(dst->szDataType, "NUMBER");
		dst->bLength = true;
		strcpy(dst->prec, "1");
		dst->bPrec = true;
		dst->bScale = false;
	}else if(strncasecmp(src->szDataType, "REAL", strlen("REAL")) == 0){
	}else if(strncasecmp(src->szDataType, "FLOAT", strlen("FLOAT")) == 0){
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
	}else if(strcasecmp(src->szDataType, "DATETIME") == 0){
		strcpy(dst->szDataType, "DATE");
	}else if(strncasecmp(src->szDataType, "BIGINT", strlen("BIGINT")) == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->prec, "20");
		dst->bPrec = true;
		dst->bScale = false;
	}else if(strncasecmp(src->szDataType, "INT", strlen("INT")) == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->prec, "19");
		dst->bPrec = true;
		dst->bScale = false;
	}else if(strncasecmp(src->szDataType, "DECIMAL", strlen("DECIMAL")) == 0){
		src->bPrec = true;
		src->bScale = true;
		strcpy(dst->szDataType, "FLOAT");
		if(atoi(src->prec) > 126){
			strcpy(dst->length, "126");
		}else{
			strcpy(dst->length, src->prec);
		}
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "BIT", strlen("BIT")) == 0){
		strcpy(dst->szDataType, "RAW");
		dst->bPrec = true;
	}else if(strncasecmp(src->szDataType, "BINARY", strlen("BINARY")) == 0){
		strcpy(dst->szDataType, "RAW");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "VARBINARY", strlen("VARBINARY")) == 0){
		strcpy(dst->szDataType, "RAW");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "DATETIME", strlen("DATETIME")) == 0){
		strcpy(dst->szDataType, "DATE");
	}else if(strncasecmp(src->szDataType, "BLOB", strlen("BLOB")) == 0){
		strcpy(dst->szDataType, "BLOB");
	}else if(strncasecmp(src->szDataType, "DOUBLE", strlen("DOUBLE")) == 0){
		strcpy(dst->szDataType, "FLOAT");
		if(atoi(src->prec) > 126){
			strcpy(dst->length, "126");
		}else{
			strcpy(dst->length, src->prec);
		}		
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "INT", strlen("INT")) == 0){
	}else if(strncasecmp(src->szDataType, "INTEGER", strlen("INTEGER")) == 0){
		strcpy(dst->szDataType, "NUMBER");
	}else if(strncasecmp(src->szDataType, "ENUM", strlen("ENUM")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "255");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "LONGBLOB", strlen("LONGBLOB")) == 0){
		strcpy(dst->szDataType, "BLOB");
	}else if(strncasecmp(src->szDataType, "LONGTEXT", strlen("LONGTEXT")) == 0){
		strcpy(dst->szDataType, "CLOB");
	}else if(strncasecmp(src->szDataType, "MEDIUMBLOB", strlen("MEDIUMBLOB")) == 0){
		strcpy(dst->szDataType, "BLOB");
	}else if(strncasecmp(src->szDataType, "MEDIUMINT", strlen("MEDIUMINT")) == 0){
		strcpy(dst->szDataType, "NUMBER");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strncasecmp(src->szDataType, "MEDIUMTEXT", strlen("MEDIUMTEXT")) == 0){
		strcpy(dst->szDataType, "CLOB");
	}else if(strncasecmp(src->szDataType, "NUMERIC", strlen("NUMERIC")) == 0){
		strcpy(dst->szDataType, "NUMBER");
	}else if(strncasecmp(src->szDataType, "SET", strlen("SET")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "255");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "SMALLINT", strlen("SMALLINT")) == 0){
	}else if(strncasecmp(src->szDataType, "TEXT", strlen("TEXT")) == 0){
		strcpy(dst->szDataType, "CLOB");
	}else if(strncasecmp(src->szDataType, "TIMESTAMP", strlen("TIMESTAMP")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "TIME", strlen("TIME")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "TINYBLOB", strlen("TINYBLOB")) == 0){
		strcpy(dst->szDataType, "BLOB");
		//dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "TINYINT", strlen("TINYINT")) == 0){
		strcpy(dst->szDataType, "NUMBER");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strncasecmp(src->szDataType, "TINYTEXT", strlen("TINYTEXT")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "VARCHAR", strlen("VARCHAR")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "YEAR", strlen("YEAR")) == 0){
		strcpy(dst->szDataType, "NUMBER");
	}else if(strncasecmp(src->szDataType, "POINT", strlen("POINT")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "LINESTRING", strlen("LINESTRING")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "POLYGON", strlen("POLYGON")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "GEOMETRY", strlen("GEOMETRY")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "MULTIPOINT", strlen("MULTIPOINT")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "4000");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "MULTILINESTRING", strlen("MULTILINESTRING")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "4000");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "MULTIPOLYGON", strlen("MULTIPOLYGON")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "4000");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "GEOMETRYCOLLECTION", strlen("GEOMETRYCOLLECTION")) == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		strcpy(dst->length, "4000");
		dst->bLength = true;
	}
	//NVARCHAR长度要大于0
	if(strncasecmp(dst->szDataType, "NVARCHAR", strlen("NVARCHAR")) == 0){
		if(dst->length && (strcasecmp(dst->length, "0") == 0)) {
			strcpy(dst->length, "1");
		}
	}
	//mssql端Float最大长度为53
	else if (strncasecmp(dst->szDataType, "FLOAT", strlen("FLOAT")) == 0){
		if(dst->prec && (atoi(dst->prec) > 53)) {
			strcpy(dst->prec, "53");
		}
	}
}

static void MiMysqlToMssql(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(src->szDataType, "BIT") == 0){
		strcpy(dst->szDataType, "BINARY");
		strcpy(dst->length, src->prec);
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "INT") == 0){
	}else if(strcasecmp(src->szDataType, "INTEGER") == 0){
		strcpy(dst->szDataType, "INT");
	}else if(strcasecmp(src->szDataType, "BINARY") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "BLOB") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "BOOL") == 0){
		strcpy(dst->szDataType, "BIT");
	}else if(strcasecmp(src->szDataType, "BOOLEAN") == 0){
		strcpy(dst->szDataType, "BIT");
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		strcpy(dst->szDataType, "NCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CHARACTER") == 0){
		strcpy(dst->szDataType, "NCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CHAR BYTE") == 0){
		strcpy(dst->szDataType, "NCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "VARCHAR") == 0 
		|| strcasecmp(src->szDataType, "NVARCHAR") == 0){
			strcpy(dst->szDataType, "NVARCHAR");
			dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "DOUBLE") == 0 
		|| strcasecmp(src->szDataType, "REAL") == 0){
			if( src->prec && atoi(src->prec) <= 53 ){
				strcpy(dst->szDataType, "FLOAT");
			}else{
				strcpy(dst->szDataType, "VARCHAR");
				dst->bScale = false;
			}		
			dst->bPrec = true;
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0  
		|| strcasecmp(src->szDataType, "NUMERIC") == 0){
			src->bPrec = true;
			src->bScale = true;
			if(src->prec && atoi(src->prec) <= 38){
				strcpy(dst->szDataType, "DECIMAL");
				dst->bPrec = true;
				dst->bScale = true;
			}else{
				strcpy(dst->szDataType, "VARCHAR");
				dst->bPrec = true;
				dst->bScale = false;
			}		
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
		if(src->prec == NULL)
			return;

		if(atoi(src->prec) <= 24){
			strcpy(dst->szDataType, "FLOAT");
			strcpy(dst->prec, "24");
		}else if(atoi(src->prec) > 24 && atoi(src->prec) <= 53){
			strcpy(dst->szDataType, "FLOAT");
		}else{
			strcpy(dst->szDataType, "VARCHAR");			
		}
		dst->bPrec = true;
		dst->bScale = false;
	}else if(strcasecmp(src->szDataType, "LONGBLOB") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "LONGTEXT") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "MEDIUMBLOB") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "MEDIUMINT") == 0){
		strcpy(dst->szDataType, "INT");
	}else if(strcasecmp(src->szDataType, "MEDIUMTEXT") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "TEXT") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		strcpy(dst->length, "MAX");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "TIMESTAMP") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		dst->bLength = true;
		strcpy(dst->length, "64");
	}else if(strcasecmp(src->szDataType, "TINYBLOB") == 0){
		strcpy(dst->szDataType, "VARBINARY");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "TINYINT") == 0){
		strcpy(dst->szDataType, "NUMERIC");
	}else if(strcasecmp(src->szDataType, "TINYTEXT") == 0){
		strcpy(dst->szDataType, "NVARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "SMALLINT") == 0){
		strcpy(dst->szDataType, "NUMERIC");
	}else if(strcasecmp(src->szDataType, "YEAR") == 0){
		strcpy(dst->szDataType, "SMALLINT");
	}else if(strcasecmp(src->szDataType, "SERIAL") == 0){
		strcpy(dst->szDataType, "BIGINT");
	}else if(strncasecmp(src->szDataType, "SET", strlen("SET")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "TIME", strlen("TIME")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"16");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "ENUM", strlen("ENUM")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		//strcpy(dst->length ,"64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "VARCHAR2", strlen("VARCHAR2")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		//strcpy(dst->length ,"64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "POINT", strlen("POINT")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "LINESTRING", strlen("LINESTRING")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "POLYGON", strlen("POLYGON")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "GEOMETRY", strlen("GEOMETRY")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"64");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "MULTIPOINT", strlen("MULTIPOINT")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"MAX");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "MULTILINESTRING", strlen("MULTILINESTRING")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"MAX");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "MULTIPOLYGON", strlen("MULTIPOLYGON")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"MAX");
		dst->bLength = true;
	}else if(strncasecmp(src->szDataType, "GEOMETRYCOLLECTION", strlen("GEOMETRYCOLLECTION")) == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length ,"MAX");
		dst->bLength = true;
	}

	if(strcasecmp(dst->szDataType, "BIT") == 0)
	{
		if(dst->prec)
		{
			int precision = atoi(dst->prec);
			int die = precision / 8;
			int remainder = precision % 8;
			if(remainder != 0)
				die ++;
			if(die == 0)
				die++;

			sprintf(dst->prec, "%d", die);
		}else{
			strcpy(dst->prec, "1");			
		}
		dst->bPrec = true;
	}else if(strcasecmp(dst->szDataType, "BINARY") == 0)
	{
		if(atoi(dst->length) > 8000){
			strcpy(dst->length, "MAX");			
		}
		dst->bLength = true;
	}else if(strcasecmp(dst->szDataType, "NUMERIC") == 0)
	{
		dst->bScale = true;
		if(dst->prec && atoi(dst->prec) > 38){
			strcpy(dst->szDataType, "VARCHAR");
			dst->bScale = false;
		}
		dst->bPrec = true;

	}else if(strcasecmp(dst->szDataType, "NVARCHAR") == 0)
	{
		if( atoi(dst->length) > 4000 ){
			strcpy(dst->length, "MAX");			
		}
		dst->bLength = true;
	}else if(strcasecmp(dst->szDataType, "VARBINARY") == 0)
	{
		if(atoi(dst->length) > 8000){
			strcpy(dst->length, "MAX");			
		}
		dst->bLength = true;
	}
}

static void MiMssqlToMssql(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(src->szDataType, "IMAGE") == 0){
	}else if(strcasecmp(src->szDataType, "TEXT") == 0){
	}else if(strcasecmp(src->szDataType, "UNIQUEIDENTIFIER") == 0){
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
	}else if(strcasecmp(src->szDataType, "TIME") == 0){
	}else if(strcasecmp(src->szDataType, "DATETIME2") == 0){
	}else if(strcasecmp(src->szDataType, "DATETIMEOFFSET") == 0){
	}else if(strcasecmp(src->szDataType, "TINYINT") == 0){
	}else if(strcasecmp(src->szDataType, "SMALLINT") == 0){
	}else if(strcasecmp(src->szDataType, "INT") == 0){
	}else if(strcasecmp(src->szDataType, "SMALLDATETIME") == 0){
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
	}else if(strcasecmp(src->szDataType, "MONEY") == 0){
	}else if(strcasecmp(src->szDataType, "DATETIME") == 0){
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
		dst->bPrec = true;
	}else if(strcasecmp(src->szDataType, "SQL_VARIANT") == 0){
	}else if(strcasecmp(src->szDataType, "NTEXT") == 0){
	}else if(strcasecmp(src->szDataType, "BIT") == 0){
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "NUMERIC") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "SMALLMONEY") == 0){
	}else if(strcasecmp(src->szDataType, "BIGINT") == 0){
	}else if(strcasecmp(src->szDataType, "HIERARCHYID") == 0){
	}else if(strcasecmp(src->szDataType, "GEOMETRY") == 0){
	}else if(strcasecmp(src->szDataType, "GEOGRAPHY") == 0){
	}else if(strcasecmp(src->szDataType, "VARBINARY") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "VARCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "BINARY") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "TIMESTAMP") == 0){
	}else if(strcasecmp(src->szDataType, "NVARCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "XML") == 0){
	}else if(strcasecmp(src->szDataType, "SYSNAME") == 0){
	}
}

static void MiMssqlToOracle(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(dst->length, "MAX") == 0){
		strcpy(dst->length, "4000");
	}

	if(strcasecmp(src->szDataType, "IMAGE") == 0){
	}else if(strcasecmp(src->szDataType, "TEXT") == 0){
	}else if(strcasecmp(src->szDataType, "UNIQUEIDENTIFIER") == 0){
		strcpy(dst->szDataType, "RAW");
		strcpy(dst->length, "16");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
	}else if(strcasecmp(src->szDataType, "TIME") == 0){
		strcpy(dst->szDataType, "DATE");
	}else if(strcasecmp(src->szDataType, "DATETIME2") == 0){
		strcpy(dst->szDataType, "TIMESTAMP");
	}else if(strcasecmp(src->szDataType, "DATETIMEOFFSET") == 0){
	}else if(strcasecmp(src->szDataType, "TINYINT") == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->prec, "3");
		dst->bPrec = true;
	}else if(strcasecmp(src->szDataType, "SMALLINT") == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->prec, "5");
		dst->bPrec = true;
	}else if(strcasecmp(src->szDataType, "INT") == 0){
	}else if(strcasecmp(src->szDataType, "SMALLDATETIME") == 0){
		strcpy(dst->szDataType, "TIMESTAMP");
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
		strcpy(dst->szDataType, "FLOAT");
	}else if(strcasecmp(src->szDataType, "MONEY") == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->prec, "15");
		strcpy(dst->scale, "2");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "DATETIME") == 0){
		strcpy(dst->szDataType, "DATE");
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
		dst->bPrec = true;
	}else if(strcasecmp(src->szDataType, "SQL_VARIANT") == 0){
	}else if(strcasecmp(src->szDataType, "NTEXT") == 0){
	}else if(strcasecmp(src->szDataType, "BIT") == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->length, "1");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "NUMERIC") == 0){
		strcpy(dst->szDataType, "NUMBER");
	}else if(strcasecmp(src->szDataType, "SMALLMONEY") == 0){
	}else if(strcasecmp(src->szDataType, "BIGINT") == 0){
		strcpy(dst->szDataType, "NUMBER");
		strcpy(dst->prec, "38");
		strcpy(dst->scale, "0");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "HIERARCHYID") == 0){
	}else if(strcasecmp(src->szDataType, "GEOMETRY") == 0){
	}else if(strcasecmp(src->szDataType, "GEOGRAPHY") == 0){
	}else if(strcasecmp(src->szDataType, "VARBINARY") == 0){
		strcpy(dst->szDataType, "BLOB");
	}else if(strcasecmp(src->szDataType, "VARCHAR") == 0){
		strcpy(dst->szDataType, "VARCHAR2");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "BINARY") == 0){
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "TIMESTAMP") == 0){
		strcpy(dst->szDataType, "DATE");
	}else if(strcasecmp(src->szDataType, "NVARCHAR") == 0){
		strcpy(dst->szDataType, "NVARCHAR2");
		if(dst->length && atoi(dst->length) > 2000)
			strcpy(dst->length, "2000");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "XML") == 0){
	}else if(strcasecmp(src->szDataType, "SYSNAME") == 0){
	}
}

static void MiMssqlToMysql(TypeInfo *src, TypeInfo *dst)
{
	memcpy(dst, src, sizeof(TypeInfo));
	dst->bLength = false;
	dst->bPrec   = false;
	dst->bScale  = false;

	if(strcasecmp(dst->length, "MAX") == 0){
		strcpy(dst->length, "8000");
	}

	if(strcasecmp(src->szDataType, "IMAGE") == 0){
	}else if(strcasecmp(src->szDataType, "TEXT") == 0){
	}else if(strcasecmp(src->szDataType, "UNIQUEIDENTIFIER") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		strcpy(dst->length, "40");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "DATE") == 0){
	}else if(strcasecmp(src->szDataType, "TIME") == 0){
	}else if(strcasecmp(src->szDataType, "DATETIME2") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "DATETIMEOFFSET") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "TINYINT") == 0){
	}else if(strcasecmp(src->szDataType, "SMALLINT") == 0){
	}else if(strcasecmp(src->szDataType, "INT") == 0){
	}else if(strcasecmp(src->szDataType, "SMALLDATETIME") == 0){
		strcpy(dst->szDataType, "DATETIME");
	}else if(strcasecmp(src->szDataType, "REAL") == 0){
		strcpy(dst->szDataType, "FLOAT");
	}else if(strcasecmp(src->szDataType, "MONEY") == 0){
		strcpy(dst->szDataType, "FLOAT");
	}else if(strcasecmp(src->szDataType, "DATETIME") == 0){
	}else if(strcasecmp(src->szDataType, "FLOAT") == 0){
	}else if(strcasecmp(src->szDataType, "SQL_VARIANT") == 0){
	}else if(strcasecmp(src->szDataType, "NTEXT") == 0){
		strcpy(dst->szDataType, "TEXT");
	}else if(strcasecmp(src->szDataType, "BIT") == 0){
		strcpy(dst->szDataType, "TINYINT");
	}else if(strcasecmp(src->szDataType, "DECIMAL") == 0){
	}else if(strcasecmp(src->szDataType, "NUMERIC") == 0){
		strcpy(dst->szDataType, "DECIMAL");
		dst->bPrec = true;
		dst->bScale = true;
	}else if(strcasecmp(src->szDataType, "SMALLMONEY") == 0){
		strcpy(dst->szDataType, "FLOAT");
	}else if(strcasecmp(src->szDataType, "BIGINT") == 0){
	}else if(strcasecmp(src->szDataType, "HIERARCHYID") == 0){
	}else if(strcasecmp(src->szDataType, "GEOMETRY") == 0){
	}else if(strcasecmp(src->szDataType, "GEOGRAPHY") == 0){
	}else if(strcasecmp(src->szDataType, "VARBINARY") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "VARCHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "BINARY") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "CHAR") == 0){
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "TIMESTAMP") == 0){
	}else if(strcasecmp(src->szDataType, "NVARCHAR") == 0){
		strcpy(dst->szDataType, "VARCHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "NCHAR") == 0){
		strcpy(dst->szDataType, "CHAR");
		dst->bLength = true;
	}else if(strcasecmp(src->szDataType, "XML") == 0){
		strcpy(dst->szDataType, "TEXT");
	}else if(strcasecmp(src->szDataType, "SYSNAME") == 0){
	}
}

