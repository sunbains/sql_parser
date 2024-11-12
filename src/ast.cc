#include "sql/ast.h"

namespace {
  /* Helper function to join strings with a separator */
  std::string join(const std::vector<std::string>& items, const std::string& sep) {
    std::string result;

    for (size_t i{}; i < items.size(); ++i) {
      if (i > 0) {
        result += sep;
      }
      result += items[i];
    }

    return result;
  }
}

namespace sql {

std::string Literal::to_string() const noexcept {
  switch (m_type) {
    case Type::null:
      return "NULL";
    case Type::integer:
      return m_value;
    case Type::floating:
      return m_value;
    case Type::string:
      return "'" + m_value + "'";
    case Type::boolean:
      return m_value;
  }
  return "<?>";
}

std::string Column_ref::to_string() const noexcept {
  std::string result;

  if (m_table_name) {
    result += *m_table_name + ".";
  }

  result += m_column_name;

  if (m_alias) {
    result += " AS " + *m_alias;
  }

  return result;
} 

std::string Base_table_ref::to_string() const noexcept {
  std::string result;

  if (m_schema_name) {
    result += *m_schema_name + ".";
  }

  result += m_table_name;

  if (m_alias) {
    result += " AS " + *m_alias;
  }

  return result;
}

std::string Derived_table_ref::to_string() const noexcept {
  std::string result = "(";

  result += m_subquery->to_string();
  result += ")";

  if (m_alias) {
    result += " AS " + *m_alias;
  }

  return result;
}

std::string to_string(Join_type type) noexcept {
  switch (type) {
    case Join_type::inner:
      return "INNER JOIN ";
    case Join_type::left:
      return "LEFT JOIN ";
    case Join_type::right:
      return "RIGHT JOIN ";
    case Join_type::full:
      return "FULL JOIN ";
      break;
    case Join_type::cross:
      return "CROSS JOIN ";
  }

  return "";
}

std::string Join::to_string() const noexcept {
  std::string result;

  result += m_left->to_string();
  result += "\n";

  if (m_natural) {
    result += "NATURAL ";
  }

  result += sql::to_string(m_type);

  result += m_right->to_string();

  if (!m_natural && m_type != Join_type::cross) {
    result += "\nON ";

    if (std::holds_alternative<std::unique_ptr<Binary_op>>(m_condition)) {
      result += std::get<std::unique_ptr<Binary_op>>(m_condition)->to_string();
    } else if (std::holds_alternative<std::unique_ptr<Using_clause>>(m_condition)) {
      result += "USING (";

      const auto& using_cols = std::get<std::unique_ptr<Using_clause>>(m_condition)->m_columns;

      result += join(using_cols, ", ");
      result += ")";
    }
  }

  return result;
}

std::string Join_ref::to_string() const noexcept {
  return m_join->to_string();
}

std::string Using_clause::to_string() const noexcept {
  return "USING (" + join(m_columns, ", ") + ")";
}

std::string to_string(Binary_op::Op_type op) noexcept {
  using Op_type = Binary_op::Op_type;

  switch (op) {
    case Op_type::EQ:
      return "=";
    case Op_type::NEQ:
      return "<>";
    case Op_type::LT:
      return "<";
    case Op_type::GT:
      return ">";
    case Op_type::LTE:
      return "<=";
    case Op_type::GTE:
      return ">=";
    case Op_type::ADD:
      return "+";
    case Op_type::SUBTRACT:
      return "-";
    case Op_type::MULTIPLY:
      return "*";
    case Op_type::DIVIDE:
      return "/";
    case Op_type::MOD:
      return "%";
    case Op_type::AND:
      return "AND";
    case Op_type::OR:
      return "OR";
    case Op_type::LIKE:
      return "LIKE";
    case Op_type::IN:
      return "IN";
    case Op_type::COMMA:
      return ",";
  }

  return "";
}

std::string Binary_op::to_string() const noexcept {
  return m_left->to_string() + " " + sql::to_string(m_op) + " " + m_right->to_string();
}

std::string Where_clause::to_string() const noexcept {
  if (!m_condition) {
    return "";
  } else {
    return "WHERE " + m_condition->to_string();
  }
}

std::string Order_by_item::to_string() const noexcept {
  std::string result = m_column->to_string();
  
  if (!m_ascending) {
    result += " DESC";
  }

  if (m_nulls) {
    result += " NULLS " + *m_nulls;
  }

  return result;
}

std::string Group_by::to_string() const noexcept {
  std::string result = "GROUP BY ";

  for (size_t i{}; i < m_columns.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += m_columns[i]->to_string();
  }

  if (m_having) {
    result += "\nHAVING " + m_having->to_string();
  }

  return result;
}

std::string Select_stmt::to_string() const noexcept {
  std::string result = "SELECT ";

  if (m_distinct) {
    result += "DISTINCT ";
  }
    
  /* Columns */
  for (size_t i{}; i < m_columns.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += m_columns[i]->to_string();
  }
    
  /* FROM clause */
  result += "\nFROM ";
  for (size_t i{}; i < m_from.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += m_from[i]->to_string();
  }
    
  /* Optional clauses */
  if (m_where) {
    result += "\n" + m_where->to_string();
  }
    
  if (m_group_by) {
    result += "\n" + m_group_by->to_string();
  }
    
  if (!m_order_by.empty()) {
    result += "\nORDER BY ";
    result += "\nORDER BY ";

    for (size_t i{}; i < m_order_by.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_order_by[i]->to_string();
    }
  }
    
  if (m_limit) {
    result += "\nLIMIT " + std::to_string(*m_limit);
  }
    
  return result;
}

std::string Insert_stmt::to_string() const noexcept {
  std::string result = "INSERT INTO ";
    
  /* Table name */
  result += m_table_name;
    
  /* Optional column list */
  if (!m_columns.empty()) {
    result += " (";
    for (size_t i{}; i < m_columns.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_columns[i];
    }
    result += ")";
  }
    
  result += "\n";
    
  using Select_stmt_ptr = std::unique_ptr<Select_stmt>;
  using Value_lists = std::vector<std::vector<std::unique_ptr<Ast_base>>>;

  /* Values or SELECT */
  if (std::holds_alternative<Value_lists>(m_values)) {
    const auto& value_lists = std::get<Value_lists>(m_values);
    result += "VALUES ";
      
    /* Print each row of values */
    for (size_t i{}; i < value_lists.size(); ++i) {
      if (i > 0) {
        /* Align multiple rows */
        result += ",\n       ";
      }
        
      result += "(";
      const auto& row = value_lists[i];

      for (size_t j{}; j < row.size(); ++j) {
        if (j > 0) {
          result += ", ";
        }
        result += row[j]->to_string();
      }
      result += ")";
    }

  } else if (std::holds_alternative<Select_stmt_ptr>(m_values)) {
    const auto& select = std::get<Select_stmt_ptr>(m_values);
    result += select->to_string();
  }
    
  /* Optional ON DUPLICATE KEY UPDATE */
  if (m_on_duplicate) {
    result += "\nON DUPLICATE KEY UPDATE ";
    const auto& assignments = *m_on_duplicate;
      
    for (size_t i{}; i < assignments.size(); ++i) {
      if (i > 0) {
        /* Align multiple assignments */
        result += ",\n                        ";
      }
        
      result += assignments[i].m_column;
      result += " = ";
      result += assignments[i].m_value->to_string();
    }
  }
    
  return result;
}

std::string Update_stmt::to_string() const noexcept {
  std::string result = "UPDATE ";
    
  /* Table reference */
  result += m_table->to_string();
  result += "\n";
    
  /* SET clause with assignments */
  result += "SET ";
  bool first_assignment = true;
  const size_t indent = 4;  // Indentation for wrapped lines
    
  for (const auto& assignment : m_assignments) {
    if (!first_assignment) {
      result += ",\n" + std::string(indent, ' ');
    }
      
    result += assignment.m_column;
    result += " = ";
    result += assignment.m_value->to_string();
      
    first_assignment = false;
  }
    
  /* WHERE clause */
  if (m_where) {
    result += "\n";
    result += m_where->to_string();
  }
    
  /* ORDER BY */
  if (!m_order_by.empty()) {
    result += "\nORDER BY ";
    for (size_t i{}; i < m_order_by.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_order_by[i]->to_string();
    }
  }
   
  /* LIMIT clause */
  if (m_limit) {
    result += "\nLIMIT ";
    result += std::to_string(*m_limit);
  }

  return result;
}

std::string Delete_stmt::to_string() const noexcept {
  std::string result = "DELETE FROM ";
    
  /* Table reference */
  result += m_table->to_string();
    
  /* USING clause */
  if (m_using && !m_using->empty()) {
    result += "\nUSING ";
      
    for (size_t i{}; i < m_using->size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += (*m_using)[i]->to_string();
    }
  }
    
  /* WHERE clause */
  if (m_where) {
    result += "\n";
    result += m_where->to_string();
  }
    
  /* ORDER BY */
  if (!m_order_by.empty()) {
    result += "\nORDER BY ";

    for (size_t i{}; i < m_order_by.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_order_by[i]->to_string();
    }
  }
    
  /* LIMIT clause */
  if (m_limit) {
    result += "\nLIMIT " + std::to_string(*m_limit);
  }
    
  return result;
}

std::string to_string(Data_type::Type type) noexcept {
  using Type = Data_type::Type;

  switch (type) {
    case Data_type::Type::integer:
      return "INT";
    case Type::bigint:
      return "BIGINT";
    case Type::smallint:
      return "SMALLINT";
    case Type::decimal:
      return "DECIMAL";
    case Type::numeric:
      return "NUMERIC";
    case Type::float_:
      return "FLOAT";
    case Type::char_:
      return "CHAR";
    case Type::varchar:
      return "VARCHAR";
    case Type::text:
      return "TEXT";
    case Type::date:
      return "DATE";
    case Type::json:
      return "JSON";
    case Type::blob:
      return "BLOB";
    case Type::boolean:
      return "BOOLEAN";
    case Type::double_:
      return "DOUBLE";
    case Type::timestamp:
      return "TIMESTAMP";
    case Type::time:
      return "TIME";
  }

  return "";
}

std::string Data_type::to_string() const noexcept {
  std::string result = sql::to_string(m_base_type);
    
  /* Add length/precision if specified */
  if (m_length) {
    result += "(" + std::to_string(*m_length);

    if (m_scale) {
      result += ", " + std::to_string(*m_scale);
    }

    result += ")";
  }
    
  /* Add character set if specified */
  if (m_charset) {
    result += " CHARACTER SET " + *m_charset;
  }
    
  return result;
}



std::string to_string(Foreign_key_reference::Action action) noexcept {
  using Action = Foreign_key_reference::Action;

  switch (action) {
    case Action::no_action:
      return "NO ACTION";
    case Action::restrict:
      return "RESTRICT";
    case Action::cascade:
        return "CASCADE";
    case Action::set_null:
      return "SET NULL";
    case Action::set_default:
      return "SET DEFAULT";
  }

  return "";
}

std::string Create_stmt::to_string() const noexcept {
  std::string result = "CREATE ";
    
  /* Optional modifiers */
  if (m_or_replace) {
    result += "OR REPLACE ";
  }
    
  if (m_temporary) {
    result += "TEMPORARY ";
  }
    
  /* Object type */
  switch (m_object_type) {
    case Object_type::table:
      result += "TABLE ";
      break;
    case Object_type::index:
      result += "INDEX ";
      break;
    case Object_type::view:
      result += "VIEW ";
      break;
    case Object_type::sequence:
      result += "SEQUENCE ";
      break;
    case Object_type::trigger:
      result += "TRIGGER ";
      break;
    case Object_type::procedure:
      result += "PROCEDURE ";
      break;
    case Object_type::function:
      result += "FUNCTION ";
      break;
  }
    
  /* IF NOT EXISTS */
  if (m_if_not_exists) {
    result += "IF NOT EXISTS ";
  }
  
  std::visit([&result](const auto& arg) { result += arg->to_string(); }, m_definition);

  return result;
}

std::string Create_index_def::to_string() const noexcept {
  std::string result = m_index_name + " ON " + m_table_name + " (";
    
  for (size_t i{}; i < m_columns.size(); ++i) {
    const auto& col = m_columns[i];

    if (i > 0) {
      result += ", ";
    }
      
    if (col.m_column_name) {
      result += *col.m_column_name;
    } else if (col.m_expression) {
      result += "(" + col.m_expression->to_string() + ")";
    }
      
    if (col.m_length) {
      result += "(" + std::to_string(*col.m_length) + ")";
    }
      
    if (!col.m_ascending) {
      result += " DESC";
    }
  }
    
  result += ")";
    
  if (m_index_type) {
    result += " USING " + *m_index_type;
  }
    
  return result;
}

std::string to_string(Create_view_def::Algorithm algorithm) noexcept {
  using Algorithm = Create_view_def::Algorithm;

  switch (algorithm) {
    case Algorithm::undefined:
      return "UNDEFINED";
    case Algorithm::merge:
      return "MERGE";
    case Algorithm::temptable:
      return "TEMPTABLE";
  }
  return "";
}

std::string to_string(Create_view_def::Security security) noexcept {
  using Security = Create_view_def::Security;

  switch (security) {
    case Security::definer:
      return "DEFINER";
    case Security::invoker: 
      return "INVOKER";
  }

  return "";
}

std::string to_string(Create_view_def::Check_option check_option) noexcept {
  using Check_option = Create_view_def::Check_option;

  switch (check_option) {
    case Check_option::local:
      return "LOCAL";
    case Check_option::cascaded:
      return "CASCADED";
  }

  return "";
}

std::string Create_view_def::to_string() const noexcept {
  std::string result = m_view_name;
    
  if (!m_column_names.empty()) {
    result += " (";
    result += join(m_column_names, ", ");
    result += ")";
  }
    
  result += "\nAS ";

  if (m_select) {
    result += m_select->to_string();
  }
    
  if (m_with_check_option) {
    result += "\nWITH ";

    if (m_check_option) {
      result += sql::to_string(*m_check_option);
    }
    result += "CHECK OPTION";
  }
    
  return result;
}

std::string Create_table_def::to_string() const noexcept {
  std::string result = m_table_name;
  result += " (\n";
  
  const std::string indent = "    ";  // 4 spaces
  bool need_comma = false;
  
  /* Format columns */
  for (const auto& column : m_columns) {
    if (need_comma) {
      result += ",\n";
    }
    result += indent + column->to_string();
    need_comma = true;
  }
  
  /* Format constraints */
  for (const auto& constraint : m_constraints) {
    if (need_comma) {
      result += ",\n";
    }
    result += indent + constraint->to_string();
    need_comma = true;
  }
  
  result += "\n)";
  
  /* Format table options */
  std::vector<std::string> options;
  
  if (m_options.m_engine) {
    options.push_back("ENGINE = " + *m_options.m_engine);
  }
  
  if (m_options.m_charset) {
    options.push_back("DEFAULT CHARSET = " + *m_options.m_charset);
  }
  
  if (m_options.m_collate) {
    options.push_back("COLLATE = " + *m_options.m_collate);
  }
  
  if (m_options.m_auto_increment) {
    options.push_back("AUTO_INCREMENT = " + std::to_string(*m_options.m_auto_increment));
  }
  
  if (m_options.m_comment) {
    options.push_back("COMMENT = '" + *m_options.m_comment + "'");
  }
  
  if (m_options.m_row_format) {
    options.push_back("ROW_FORMAT = " + *m_options.m_row_format);
  }
  
  if (m_options.m_key_block_size) {
    options.push_back("KEY_BLOCK_SIZE = " + std::to_string(*m_options.m_key_block_size));
  }
  
  if (m_options.m_data_directory) {
    options.push_back("DATA DIRECTORY = '" + *m_options.m_data_directory + "'");
  }

  if (m_options.m_index_directory) {
    options.push_back("INDEX DIRECTORY = '" + *m_options.m_index_directory + "'");
  }

  if (m_options.m_tablespace) {
    options.push_back("TABLESPACE = " + *m_options.m_tablespace);
  }

  if (m_options.m_compression) {
    options.push_back("COMPRESSION = " + *m_options.m_compression);
  }

  if (m_options.m_avg_row_length) {
    options.push_back("AVG_ROW_LENGTH = " + std::to_string(*m_options.m_avg_row_length));
  }

  if (m_options.m_max_rows) {
    options.push_back("MAX_ROWS = " + std::to_string(*m_options.m_max_rows));
  }

  if (m_options.m_min_rows) {
    options.push_back("MIN_ROWS = " + std::to_string(*m_options.m_min_rows));
  }
  
  if (m_options.m_row_format) {
    options.push_back("ROW_FORMAT = " + *m_options.m_row_format);
  }

  if (m_options.m_key_block_size) {
    options.push_back("KEY_BLOCK_SIZE = " + std::to_string(*m_options.m_key_block_size));
  }

  if (!options.empty()) {
    result += "\n" + join(options, "\n");
  }

  /* Format partition info if present */
  if (m_partition.m_type) {
    result += "\n" + m_partition.to_string();
  }
  
  return result;
}

std::string Column_def::to_string() const noexcept {
  std::vector<std::string> parts;
  
  /* Column name and type */
  parts.push_back(m_name + " " + m_type.to_string());
  
  /* Column attributes */
  if (!m_nullable) {
    parts.push_back("NOT NULL");
  }
  
  if (m_default_value) {
    parts.push_back("DEFAULT " + m_default_value->to_string());
  }
  
  if (m_auto_increment) {
    parts.push_back("AUTO_INCREMENT");
  }
  
  if (m_primary_key) {
    parts.push_back("PRIMARY KEY");
  }
  
  if (m_unique) {
    parts.push_back("UNIQUE");
  }
  
  if (m_check) {
    parts.push_back("CHECK (" + m_check->to_string() + ")");
  }
  
  if (m_references) {
    parts.push_back(m_references->to_string());
  }
  
  if (m_comment) {
    parts.push_back("COMMENT '" + *m_comment + "'");
  }

  if (m_storage) {
    parts.push_back("STORED " + *m_storage);
  }

  if (m_generation_expr) {
    parts.push_back("GENERATED ALWAYS AS (" + m_generation_expr->to_string() + ")");
  }
  
  return join(parts, " ");
}

std::string to_string(Table_constraint::Initially initially) noexcept { 
  using Initially = Table_constraint::Initially;

  switch (initially) {
    case Initially::immediate:
      return "IMMEDIATE";
    case Initially::deferred:
      return "DEFERRED";
  }

  return "";
}

std::string to_string(Table_constraint::Type type) noexcept {
  using Type = Table_constraint::Type;

  switch (type) {
    case Type::primary_key:
      return "PRIMARY KEY";
    case Type::foreign_key:
      return "FOREIGN KEY";
    case Type::unique:
      return "UNIQUE";
    case Type::check:
      return "CHECK";
  }

  return "";
} 

std::string Table_constraint::to_string() const noexcept {
  std::vector<std::string> parts;
  
  if (m_name) {
    parts.push_back("CONSTRAINT " + *m_name);
  }
  
  using Type = Table_constraint::Type;

  std::string result = sql::to_string(m_type);

  if (m_type != Type::check) {
    result += " (" + join(m_columns, ", ") + ")";

    if (m_type == Type::foreign_key && m_reference) {
      parts.push_back(m_reference->to_string());
    }

  } else if (m_check) {
    result += " (" + m_check->to_string() + ")";
  }

  parts.push_back(result);

  return join(parts, " ");
}

std::string to_string(Foreign_key_reference::Match_type type) noexcept {
  using Match_type = Foreign_key_reference::Match_type;

  switch (type) {
    case Match_type::simple:
      return "SIMPLE";
    case Match_type::full:
      return "FULL";
    case Match_type::partial:
      return "PARTIAL";
  }

  return "";
}

std::string Foreign_key_reference::to_string() const noexcept {
  std::string result = "REFERENCES " + m_table_name;
  
  if (!m_column_names.empty()) {
    result += " (" + join(m_column_names, ", ") + ")";
  }
  
  if (m_match) {
    result += " MATCH " + sql::to_string(*m_match);
  }
  
  if (m_on_delete) {
    result += " ON DELETE " + sql::to_string(*m_on_delete);
  }
  
  if (m_on_update) {
    result += " ON UPDATE " + sql::to_string(*m_on_update);
  }
  
  return result;
}

std::string to_string(Create_table_def::Partition::Type type) noexcept {
  using Type = Create_table_def::Partition::Type;

  switch (type) {
    case Type::range:
      return "RANGE";
    case Type::list:
      return "LIST";
    case Type::hash:
      return "HASH";
    case Type::key:
      return "KEY";
  }

  return "";
}

std::string Create_table_def::Partition::to_string() const {
  std::vector<std::string> parts;

  parts.push_back("PARTITION BY");
  
  if (m_type) {
    parts.push_back(sql::to_string(*m_type));
  }
  
  if (!m_columns.empty()) {
    parts.push_back("(" + join(m_columns, ", ") + ")");
  }
  
  if (m_expression) {
    parts.push_back(m_expression->to_string());
  }
  
  if (m_partitions > 1) {
    parts.push_back("PARTITIONS " + std::to_string(m_partitions));
  }
  
  return join(parts, " ");
}

std::string Function_call::to_string() const noexcept {
  std::string result = m_function_name;
  result += "(";
  
  /* Handle COUNT(*) as a special case */
  if (m_star) {
    result += "*";
  } else {
    /* Add DISTINCT if specified */
    if (m_distinct) {
      result += "DISTINCT ";
    }
    
    /* Add function arguments */
    for (size_t i = 0; i < m_arguments.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_arguments[i]->to_string();
    }
  }
  
  result += ")";
  
  /* Add window specification if present */
  if (m_window) {
    result += " OVER ";
    result += m_window->to_string();
  }
  
  return result;
}

std::string Window_spec::to_string() const noexcept {
  std::string result = "(";
  bool need_space = false;
  
  /* Named window reference */
  if (m_reference_name) {
    result += m_reference_name.value();
  }
  
  /* PARTITION BY */
  if (m_partition) {
    result += "PARTITION BY ";
    for (size_t i{}; i < m_partition->m_columns.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_partition->m_columns[i]->to_string();
    }
    need_space = true;
  }
  
  /* ORDER BY */
  if (!m_order.empty()) {
    if (need_space) {
      result += " ";
    }
    result += "ORDER BY ";
    for (size_t i{}; i < m_order.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += m_order[i]->to_string();
    }
    need_space = true;
  }
  
  /* Frame clause */
  if (m_frame) {
    if (need_space) {
      result += " ";
    }
    result += m_frame->to_string();
  }
  
  result += ")";
  return result;
}

std::string to_string(Window_spec::Frame::Type type) noexcept {
  using Type = Window_spec::Frame::Type;

  switch (type) {
    case Type::rows:
      return "ROWS";
    case Type::range: 
      return "RANGE";
    case Type::groups:
      return "GROUPS";
  }

  return "";
}

std::string to_string(Window_spec::Frame::Bound::Type type) noexcept {
  using Type = Window_spec::Frame::Bound::Type;

  switch (type) {
    case Type::current_row:
      return "CURRENT ROW";
    case Type::unbounded_preceding:
      return "UNBOUNDED PRECEDING";
    case Type::unbounded_following:
      return "UNBOUNDED FOLLOWING";
    case Type::preceding:
      return "PRECEDING";
    case Type::following:
      return "FOLLOWING";
  }

  return "";
}

std::string to_string(Window_spec::Frame::Exclude exclude) noexcept {
  using Exclude = Window_spec::Frame::Exclude;

  switch (exclude) {
    case Exclude::no_others:
      return "NO OTHERS";
    case Exclude::current_row:  
      return "CURRENT ROW";
    case Exclude::group:
      return "GROUP";
    case Exclude::ties:
      return "TIES";
  }

  return "";
}

std::string Window_spec::Frame::to_string() const noexcept {
  std::string result(sql::to_string(m_type)); 
 
  /* Frame bounds */
  result += m_start.to_string();

  if (m_end.m_type != m_start.m_type || m_end.m_offset != m_start.m_offset) {
    result += " AND ";
    result += m_end.to_string();
  }
  
  /* EXCLUDE clause */
  if (m_exclude) {
    result += " EXCLUDE " + sql::to_string(*m_exclude);
  }
  
  return result;
}

std::string Window_spec::Frame::Bound::to_string() const noexcept {
  std::string result;
  
  if ((m_type == Type::preceding || m_type == Type::following) && m_offset) {
    result += m_offset->to_string();
  }

  return result + sql::to_string(m_type);
}

std::string Alter_table_stmt::to_string() const noexcept {
  std::string result = "ALTER TABLE ";
    
  /* IF EXISTS */
  if (m_if_exists) {
    result += "IF EXISTS ";
  }
  
  /* ONLY keyword for inheritance */
  if (m_only) {
    result += "ONLY ";
  }
  
  /* Table name */
  result += m_table_name;
  
  /* ALL INHERITANCE marker */
  if (m_all_inheritance) {
    result += "*";
  }
  
  /* Add alteration */
  result += "\n" + sql::to_string(m_alteration);
  
  return result;
}

std::string Alter_table_stmt::Add_column::to_string() const noexcept {
  std::string result = "ADD COLUMN ";
  
  /* Column definition */
  result += m_column.to_string();
  
  /* Position specifier */
  if (m_first) {
    result += " FIRST";
  }
  else if (m_after_column) {
    result += " AFTER " + *m_after_column;
  }
  
  return result;
}

std::string Alter_table_stmt::Drop_column::to_string() const noexcept {
  std::string result = "DROP COLUMN " + m_column_name;
  
  if (m_cascade) {
    result += " CASCADE";
  }
  
  return result;
}

std::string Alter_table_stmt::Modify_column::to_string() const noexcept {
  return "MODIFY COLUMN " + m_column_name + " " + m_new_definition.to_string();
}

std::string to_string(Alter_table_stmt::Add_constraint::Type type) noexcept {
  using Type = Alter_table_stmt::Add_constraint::Type;

  switch (type) {
    case Type::primary_key:
      return "PRIMARY KEY";
    case Type::foreign_key:
      return "FOREIGN KEY";
    case Type::unique:
      return "UNIQUE";
    case Type::check:
      return "CHECK";
  }

  return "";  
}

std::string Alter_table_stmt::Add_constraint::Foreign_key_def::to_string() const noexcept {
  std::string result = "REFERENCES " + m_table + " ";

  if (!m_columns.empty()) {
    result += "(" + join(m_columns, ", ") + ") ";
  }
  if (!m_on_delete.empty()) {
    result += "ON DELETE " + m_on_delete + " ";
  }
  if (!m_on_update.empty()) {
    result += "ON UPDATE " + m_on_update;
  }

  return result;
}

std::string Alter_table_stmt::Add_constraint::to_string() const noexcept {
  std::string result = "ADD ";
  
  /* Optional constraint name */
  if (m_name) {
    result += "CONSTRAINT " + *m_name + " ";
  }
  
  result += sql::to_string(m_type);

  if (m_type != Type::check && !m_columns.empty()) {
    result += "(" + join(m_columns, ", ") + ")";
  } else if (m_type == Type::check && m_check_condition) {
    result += "(" + m_check_condition->to_string() + ")";
  }

  if (m_type == Type::foreign_key && m_foreign_key_def) {
    result += m_foreign_key_def->to_string();
  }

  return result;
}

std::string Alter_table_stmt::Drop_constraint::to_string() const noexcept {
  std::string result = "DROP CONSTRAINT " + m_constraint;
  
  if (m_cascade) {
    result += " CASCADE";
  }
  
  return result;
}

std::string Alter_table_stmt::Rename_column::to_string() const noexcept {
  return "RENAME COLUMN " + m_old_name + " TO " + m_new_name;
}

std::string Alter_table_stmt::Rename_table::to_string() const noexcept {
  return "RENAME TO " + m_name;
}

std::string to_string(const Alter_table_stmt::Alteration& alt) noexcept {
  std::string result;

  std::visit([&result](const auto& arg) { result += arg.to_string(); }, alt);
  
  return result;
}

std::string to_string(Object_type type) noexcept {
  using Type = Object_type; 

  switch (type) {
    case Type::table:
      return "TABLE";
    case Type::index:
      return "INDEX";
    case Type::view:
      return "VIEW";
    case Type::sequence:
      return "SEQUENCE";
    case Type::trigger:
      return "TRIGGER";
    case Type::procedure:
      return "PROCEDURE";
    case Type::function:
      return "FUNCTION";
    case Type::database:
      return "DATABASE";
  }

  return "";
}

std::string Drop_stmt::to_string() const noexcept {
  std::string result = "DROP " + sql::to_string(m_object_type);
  
  /* IF EXISTS clause */
  if (m_if_exists) {
    result += "IF EXISTS ";
  }
  
  result += " " + join(m_object_names, ", ");
  
  /* CASCADE/RESTRICT modifier */
  if (m_cascade) {
    result += " CASCADE";
  }
  
  return result;
}

std::string to_string(Grant_revoke_stmt::Object_type type) noexcept {
  using Type = Grant_revoke_stmt::Object_type;

  switch (type) {
    case Type::table:
      return "TABLE";
    case Type::view:  
      return "VIEW";
    case Type::procedure:
      return "PROCEDURE";
    case Type::function:
      return "FUNCTION";
    case Type::database:
      return "DATABASE";
  }

  return "";
}

std::string to_string(Grant_revoke_stmt::Privilege::Type type) noexcept {
  using Type = Grant_revoke_stmt::Privilege::Type;

  switch (type) {
    case Type::select:  
      return "SELECT";
    case Type::insert:
      return "INSERT";
    case Type::update:
      return "UPDATE";
    case Type::delete_:
      return "DELETE";
    case Type::truncate:
      return "TRUNCATE";
    case Type::references:
      return "REFERENCES";
    case Type::trigger:
      return "TRIGGER"; 
    case Type::all:
      return "ALL";
  }

  return "";
}

std::string to_string(Merge_stmt::When_clause::Match_type type) noexcept {
  using Type = Merge_stmt::When_clause::Match_type;

  switch (type) {
    case Type::matched:
      return "MATCHED";
    case Type::not_matched_target:
      return "NOT MATCHED TARGET";
    case Type::not_matched_source:
      return "NOT MATCHED SOURCE";
  }

  return "";
}

std::string Merge_stmt::When_clause::to_string() const noexcept {
  return sql::to_string(m_type) + " WHEN " + m_condition->to_string();
}

std::string Alter_stmt::to_string() const noexcept {
  std::string result = "ALTER " + sql::to_string(m_object_type);
    
  /* IF EXISTS clause */
  if (m_if_exists) {
    result += "IF EXISTS ";
  }
  
  /* Object definition based on type */
  if (std::holds_alternative<std::unique_ptr<Alter_table_stmt>>(m_definition)) {
    const auto& table_stmt = std::get<std::unique_ptr<Alter_table_stmt>>(m_definition);
    /* Remove the "ALTER TABLE" prefix since we already added it */
    std::string table_str = table_stmt->to_string();

    result += table_str.substr(table_str.find_first_of(' ', 11) + 1);
  }
  
  return result;
}

} // namespace sql
