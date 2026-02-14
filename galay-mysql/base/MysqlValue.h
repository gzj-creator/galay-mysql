#ifndef GALAY_MYSQL_VALUE_H
#define GALAY_MYSQL_VALUE_H

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <unordered_map>

namespace galay::mysql
{

/**
 * @brief MySQL字段类型枚举
 */
enum class MysqlFieldType : uint8_t
{
    DECIMAL     = 0x00,
    TINY        = 0x01,
    SHORT       = 0x02,
    LONG        = 0x03,
    FLOAT       = 0x04,
    DOUBLE      = 0x05,
    NULL_TYPE   = 0x06,
    TIMESTAMP   = 0x07,
    LONGLONG    = 0x08,
    INT24       = 0x09,
    DATE        = 0x0a,
    TIME        = 0x0b,
    DATETIME    = 0x0c,
    YEAR        = 0x0d,
    NEWDATE     = 0x0e,
    VARCHAR     = 0x0f,
    BIT         = 0x10,
    JSON        = 0xf5,
    NEWDECIMAL  = 0xf6,
    ENUM        = 0xf7,
    SET         = 0xf8,
    TINY_BLOB   = 0xf9,
    MEDIUM_BLOB = 0xfa,
    LONG_BLOB   = 0xfb,
    BLOB        = 0xfc,
    VAR_STRING  = 0xfd,
    STRING      = 0xfe,
    GEOMETRY    = 0xff,
};

/**
 * @brief MySQL字段标志
 */
enum MysqlFieldFlags : uint16_t
{
    NOT_NULL_FLAG       = 0x0001,
    PRI_KEY_FLAG        = 0x0002,
    UNIQUE_KEY_FLAG     = 0x0004,
    MULTIPLE_KEY_FLAG   = 0x0008,
    BLOB_FLAG           = 0x0010,
    UNSIGNED_FLAG       = 0x0020,
    ZEROFILL_FLAG       = 0x0040,
    BINARY_FLAG         = 0x0080,
    ENUM_FLAG           = 0x0100,
    AUTO_INCREMENT_FLAG = 0x0200,
    TIMESTAMP_FLAG      = 0x0400,
    SET_FLAG            = 0x0800,
    NUM_FLAG            = 0x8000,
};

/**
 * @brief 列定义
 */
class MysqlField
{
public:
    MysqlField() = default;
    MysqlField(std::string name, MysqlFieldType type, uint16_t flags,
               uint32_t column_length, uint8_t decimals);

    const std::string& name() const { return m_name; }
    MysqlFieldType type() const { return m_type; }
    uint16_t flags() const { return m_flags; }
    uint32_t columnLength() const { return m_column_length; }
    uint8_t decimals() const { return m_decimals; }

    void setCatalog(std::string catalog) { m_catalog = std::move(catalog); }
    void setSchema(std::string schema) { m_schema = std::move(schema); }
    void setTable(std::string table) { m_table = std::move(table); }
    void setOrgTable(std::string org_table) { m_org_table = std::move(org_table); }
    void setOrgName(std::string org_name) { m_org_name = std::move(org_name); }
    void setCharacterSet(uint16_t cs) { m_character_set = cs; }

    const std::string& catalog() const { return m_catalog; }
    const std::string& schema() const { return m_schema; }
    const std::string& table() const { return m_table; }
    const std::string& orgTable() const { return m_org_table; }
    const std::string& orgName() const { return m_org_name; }
    uint16_t characterSet() const { return m_character_set; }

    bool isNotNull() const { return m_flags & NOT_NULL_FLAG; }
    bool isPrimaryKey() const { return m_flags & PRI_KEY_FLAG; }
    bool isAutoIncrement() const { return m_flags & AUTO_INCREMENT_FLAG; }
    bool isUnsigned() const { return m_flags & UNSIGNED_FLAG; }

private:
    std::string m_catalog;
    std::string m_schema;
    std::string m_table;
    std::string m_org_table;
    std::string m_name;
    std::string m_org_name;
    uint16_t m_character_set = 0;
    uint32_t m_column_length = 0;
    MysqlFieldType m_type = MysqlFieldType::NULL_TYPE;
    uint16_t m_flags = 0;
    uint8_t m_decimals = 0;
};

/**
 * @brief 单行数据
 */
class MysqlRow
{
public:
    MysqlRow() = default;
    explicit MysqlRow(std::vector<std::optional<std::string>> values);

    size_t size() const { return m_values.size(); }
    bool empty() const { return m_values.empty(); }

    const std::optional<std::string>& operator[](size_t index) const;
    const std::optional<std::string>& at(size_t index) const;

    bool isNull(size_t index) const;
    std::string getString(size_t index, const std::string& default_val = "") const;
    int64_t getInt64(size_t index, int64_t default_val = 0) const;
    uint64_t getUint64(size_t index, uint64_t default_val = 0) const;
    double getDouble(size_t index, double default_val = 0.0) const;

    const std::vector<std::optional<std::string>>& values() const { return m_values; }

private:
    std::vector<std::optional<std::string>> m_values;
};

/**
 * @brief 完整结果集
 */
class MysqlResultSet
{
public:
    MysqlResultSet() = default;

    // 字段信息
    void addField(MysqlField field);
    void reserveFields(size_t n) { m_fields.reserve(n); }
    size_t fieldCount() const { return m_fields.size(); }
    const MysqlField& field(size_t index) const;
    const std::vector<MysqlField>& fields() const { return m_fields; }

    // 行数据
    void addRow(MysqlRow row);
    void reserveRows(size_t n) { m_rows.reserve(n); }
    size_t rowCount() const { return m_rows.size(); }
    const MysqlRow& row(size_t index) const;
    const std::vector<MysqlRow>& rows() const { return m_rows; }

    // 按列名查找列索引
    int findField(const std::string& name) const;

    // OK包信息
    void setAffectedRows(uint64_t n) { m_affected_rows = n; }
    void setLastInsertId(uint64_t id) { m_last_insert_id = id; }
    void setWarnings(uint16_t w) { m_warnings = w; }
    void setStatusFlags(uint16_t f) { m_status_flags = f; }
    void setInfo(std::string info) { m_info = std::move(info); }

    uint64_t affectedRows() const { return m_affected_rows; }
    uint64_t lastInsertId() const { return m_last_insert_id; }
    uint16_t warnings() const { return m_warnings; }
    uint16_t statusFlags() const { return m_status_flags; }
    const std::string& info() const { return m_info; }

    // 是否是结果集（有列定义）还是仅OK包
    bool hasResultSet() const { return !m_fields.empty(); }

private:
    std::vector<MysqlField> m_fields;
    std::vector<MysqlRow> m_rows;
    uint64_t m_affected_rows = 0;
    uint64_t m_last_insert_id = 0;
    uint16_t m_warnings = 0;
    uint16_t m_status_flags = 0;
    std::string m_info;
};

} // namespace galay::mysql

#endif // GALAY_MYSQL_VALUE_H
