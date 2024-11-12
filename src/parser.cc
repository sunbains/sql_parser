#include "sql/parser.h"

namespace sql {

std::optional<Binary_op::Op_type> Parser::parse_operator() {
  auto &state = m_state_stack.back();

  if(state.m_lexeme.m_type != Lexeme_type::OPERATOR) [[likely]] {
    return std::nullopt;
  } else if (state.m_lexeme.m_value == "=") {
    return Binary_op::Op_type::EQ;
  } else if (state.m_lexeme.m_value == "<>") {
    return Binary_op::Op_type::NEQ;
  } else if (state.m_lexeme.m_value == "<") {
    return Binary_op::Op_type::LT;
  } else if (state.m_lexeme.m_value == ">") {
    return Binary_op::Op_type::GT;
  } else if (state.m_lexeme.m_value == "<=") {
    return Binary_op::Op_type::LTE;
  } else if (state.m_lexeme.m_value == ">=") {
    return Binary_op::Op_type::GTE;
  } else if (state.m_lexeme.m_value == "+") {
    return Binary_op::Op_type::ADD;
  } else if (state.m_lexeme.m_value == "*") {
    return Binary_op::Op_type::MULTIPLY;
  } else if (state.m_lexeme.m_value == "/") {
    return Binary_op::Op_type::DIVIDE;
  } else if (state.m_lexeme.m_value == "%") {
    return Binary_op::Op_type::MOD;
  } else if (state.m_lexeme.m_value == "AND") {
    return Binary_op::Op_type::AND;
  } else if (state.m_lexeme.m_value == "OR") {
    return Binary_op::Op_type::OR;
  } else if (state.m_lexeme.m_value == "LIKE") {
    return Binary_op::Op_type::LIKE;
  } else if (state.m_lexeme.m_value == "IN") {
    return Binary_op::Op_type::IN;
  }

  return std::nullopt;
}

std::unique_ptr<Binary_op> Parser::parse_expression() {
  auto left = parse_term();

  while (auto op_type = parse_operator()) {
    auto op = std::make_unique<Binary_op>();

    op->m_op = op_type.value();
    advance();

    op->m_left = std::move(left);
    op->m_right = parse_term();
    left = std::move(op);
  }

  return left;
}

std::vector<std::unique_ptr<Table_ref>> Parser::parse_table_references() {
  std::vector<std::unique_ptr<Table_ref>> tables;

  do {
    auto table = parse_table_reference();
        
    /* Handle JOINs */
    for (;;) {
      std::optional<Join_type> join_type;
            
      if (match(Lexeme_type::KEYWORD, "INNER") || match(Lexeme_type::KEYWORD, "JOIN")) {
        join_type = Join_type::inner;
      } else if (match(Lexeme_type::KEYWORD, "LEFT")) {
        if (match(Lexeme_type::KEYWORD, "OUTER")) {
          (void) match(Lexeme_type::KEYWORD, "JOIN");
        }
        join_type = Join_type::left;
      } else if (match(Lexeme_type::KEYWORD, "RIGHT")) {
        if (match(Lexeme_type::KEYWORD, "OUTER")) {
          (void) match(Lexeme_type::KEYWORD, "JOIN");
        }
        join_type = Join_type::right;
      } else if (match(Lexeme_type::KEYWORD, "FULL")) {
        if (match(Lexeme_type::KEYWORD, "OUTER")) {
          (void) match(Lexeme_type::KEYWORD, "JOIN");
        }
        join_type = Join_type::full;
      }
            
      if (!join_type) {
        break;
      }

      if (!match(Lexeme_type::KEYWORD, "JOIN")) {
        throw std::runtime_error("Expected JOIN keyword");
      }

      auto join = std::make_unique<Join>();
      join->m_type = *join_type;
      join->m_left = std::move(table);
      join->m_right = parse_table_reference();
 
      /* Parse join condition */
      if (!match(Lexeme_type::KEYWORD, "ON")) {
        throw std::runtime_error("Expected ON after JOIN");
      }
      join->m_condition = parse_expression();

      throw std::runtime_error("JOINs are not supported");
    }

    tables.push_back(std::move(table));
  } while (match(Lexeme_type::OPERATOR, ","));

  return tables;
}

std::unique_ptr<Table_ref> Parser::parse_table_reference() {
  auto table = std::make_unique<Table_ref>();
  const auto &state = m_state_stack.back();

  /* Parse schema name if present */
  if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER && peek().m_type == Lexeme_type::OPERATOR && peek().m_value == ".") {
    table->m_schema_name = state.m_lexeme.m_value;

    /* Skip schema name */
    advance();

    /* Skip the dot. */
    advance();
  }

  /* Parse table name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected table name");
  }

  table->m_table_name = state.m_lexeme.m_value;
  advance();

  /* Parse optional alias */
  if (match(Lexeme_type::KEYWORD, "AS")) {
    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error("Expected identifier after AS");
    }

    table->m_alias = state.m_lexeme.m_value;
    advance();

  } else if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER) {
    /* Handle implicit alias */
    table->m_alias = state.m_lexeme.m_value;
    advance();
  }

  return table;
}

std::unique_ptr<Where_clause> Parser::parse_where_clause() {
  auto where = std::make_unique<Where_clause>();
  where->m_condition = parse_expression();

  return where;
}

std::unique_ptr<Column_ref> Parser::parse_column_ref() {
    auto col = std::make_unique<Column_ref>();
    const auto &state = m_state_stack.back();

    col->m_column_name = state.m_lexeme.m_value;

    advance();

    if (match(Lexeme_type::OPERATOR, ".")) {
        col->m_table_name = std::move(col->m_column_name);
        expect(Lexeme_type::IDENTIFIER);
        col->m_column_name = state.m_prev_lexeme.m_value;
    }

    return col;
}

std::unique_ptr<Group_by> Parser::parse_group_by() {
  auto group = std::make_unique<Group_by>();

  do {
    group->m_columns.push_back(parse_column_ref());
  } while (match(Lexeme_type::OPERATOR, ","));

  return group;
}

std::unique_ptr<Insert_stmt> Parser::parse_insert_statement() {
  auto insert = std::make_unique<Insert_stmt>();
  const auto &state = m_state_stack.back();

  /* Parse INTO keyword */
  if (!match(Lexeme_type::KEYWORD, "INTO")) {
    throw std::runtime_error("Expected INTO after INSERT");
  }

  /* Parse table name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected table name");
  }
  insert->m_table_name = state.m_lexeme.m_value;
  advance();

  /* Parse optional column list */
  if (match(Lexeme_type::OPERATOR, "(")) {
    do {
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected column name");
      }
      insert->m_columns.push_back(state.m_lexeme.m_value);
      advance();
    } while (match(Lexeme_type::OPERATOR, ","));

    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis");
    }
  }

  /* Parse VALUES or SELECT */
  if (match(Lexeme_type::KEYWORD, "VALUES")) {
    insert->m_values = parse_value_lists();
  } else if (match(Lexeme_type::KEYWORD, "SELECT")) {
    auto select = parse_select_statement();
    insert->m_values = std::move(select);
  } else {
    throw std::runtime_error("Expected VALUES or SELECT");
  }

  /* Parse optional ON DUPLICATE KEY UPDATE */
  if (match(Lexeme_type::KEYWORD, "ON")) {
    if (!match(Lexeme_type::KEYWORD, "DUPLICATE")) {
      throw std::runtime_error("Expected DUPLICATE after ON");
    }
    if (!match(Lexeme_type::KEYWORD, "KEY")) {
      throw std::runtime_error("Expected KEY after DUPLICATE");
    }
    if (!match(Lexeme_type::KEYWORD, "UPDATE")) {
      throw std::runtime_error("Expected UPDATE after KEY");
    }

    insert->m_on_duplicate = parse_update_assignments();
  }

  return insert;
}

std::vector<std::vector<std::unique_ptr<Ast_base>>> Parser::parse_value_lists() {
  std::vector<std::vector<std::unique_ptr<Ast_base>>> value_lists;

  do {
    if (!match(Lexeme_type::OPERATOR, "(")) {
      throw std::runtime_error("Expected opening parenthesis");
    }

    std::vector<std::unique_ptr<Ast_base>> values;

    do {
      values.emplace_back(parse_expression());
    } while (match(Lexeme_type::OPERATOR, ","));

    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis");
    }

    value_lists.push_back(std::move(values));

  } while (match(Lexeme_type::OPERATOR, ","));

  return value_lists;
}

std::unique_ptr<Update_stmt> Parser::parse_update_statement() {
  auto update = std::make_unique<Update_stmt>();

  /* Parse table reference */
  update->m_table = parse_table_reference();

  /* Parse SET clause */
  if (!match(Lexeme_type::KEYWORD, "SET")) {
    throw std::runtime_error("Expected SET clause");
  }

  update->m_assignments = parse_update_assignments();

  /* Parse optional WHERE clause */
  if (match(Lexeme_type::KEYWORD, "WHERE")) {
    update->m_where = parse_where_clause();
  }

  /* Parse optional ORDER BY and LIMIT */
  if (match(Lexeme_type::KEYWORD, "ORDER")) {
    if (!match(Lexeme_type::KEYWORD, "BY")) {
      throw std::runtime_error("Expected BY after ORDER");
    }
    update->m_order_by = parse_order_by();
  }

  if (match(Lexeme_type::KEYWORD, "LIMIT")) {
    update->m_limit = parse_limit();
  }

  return update;
}

std::vector<Update_stmt::Assignment> Parser::parse_update_assignments() {
  std::vector<Update_stmt::Assignment> assignments;
  const auto &state = m_state_stack.back();

  do {
    Update_stmt::Assignment assignment;

    /* Parse column name */
    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error("Expected column name");
    }
    assignment.m_column = state.m_lexeme.m_value;
    advance();

    /* Parse equals sign */
    if (!match(Lexeme_type::OPERATOR, "=")) {
      throw std::runtime_error("Expected = in assignment");
    }

    /* Parse value expression */
    assignment.m_value = parse_expression();

    assignments.push_back(std::move(assignment));
  } while (match(Lexeme_type::OPERATOR, ","));

  return assignments;
}

std::unique_ptr<Delete_stmt> Parser::parse_delete_statement() {
  auto delete_stmt = std::make_unique<Delete_stmt>();

  /* Parse FROM keyword */
  if (!match(Lexeme_type::KEYWORD, "FROM")) {
    throw std::runtime_error("Expected FROM after DELETE");
  }

  /* Parse table reference */
  delete_stmt->m_table = parse_table_reference();

  /* Parse optional USING clause for multi-table deletes */
  if (match(Lexeme_type::KEYWORD, "USING")) {
    delete_stmt->m_using = parse_table_references();
  }

  /* Parse optional WHERE clause */
  if (match(Lexeme_type::KEYWORD, "WHERE")) {
    delete_stmt->m_where = parse_where_clause();
  }

  /* Parse optional ORDER BY and LIMIT */
  if (match(Lexeme_type::KEYWORD, "ORDER")) {
    if (!match(Lexeme_type::KEYWORD, "BY")) {
      throw std::runtime_error("Expected BY after ORDER");
    }
    delete_stmt->m_order_by = parse_order_by();
  }

  if (match(Lexeme_type::KEYWORD, "LIMIT")) {
    delete_stmt->m_limit = parse_limit();
  }

  return delete_stmt;
}

std::unique_ptr<Create_stmt> Parser::parse_create_statement() {
  auto create = std::make_unique<Create_stmt>();

  /* Parse optional IF NOT EXISTS */
  if (match(Lexeme_type::KEYWORD, "IF")) {
    if (!match(Lexeme_type::KEYWORD, "NOT")) {
      throw std::runtime_error("Expected NOT after IF");
    }
    if (!match(Lexeme_type::KEYWORD, "EXISTS")) {
      throw std::runtime_error("Expected EXISTS after NOT");
    }
    create->m_if_not_exists = true;
  }

  /* Parse object type (TABLE, INDEX, VIEW, etc.) */
  if (match(Lexeme_type::KEYWORD, "TABLE")) {
    create->m_object_type = Create_stmt::Object_type::table;
    create->m_definition = parse_create_table();
  } else if (match(Lexeme_type::KEYWORD, "INDEX")) {
    create->m_object_type = Create_stmt::Object_type::index;
    create->m_definition = parse_create_index();
  } else if (match(Lexeme_type::KEYWORD, "VIEW")) {
    create->m_object_type = Create_stmt::Object_type::view;
    create->m_definition = parse_create_view();
  } else {
    throw std::runtime_error("Unsupported CREATE statement type");
  }

  return create;
}

std::unique_ptr<Create_table_def> Parser::parse_create_table() {
  auto table = std::make_unique<Create_table_def>();
  const auto &state = m_state_stack.back();

  /* Parse table name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected table name");
  }
  table->m_table_name = state.m_lexeme.m_value;
  advance();

  /* Parse column definitions */
  if (!match(Lexeme_type::OPERATOR, "(")) {
    throw std::runtime_error("Expected opening parenthesis");
  }

  do {
    /* Check if this is a constraint definition */
    if (match(Lexeme_type::KEYWORD, "CONSTRAINT") ||
        match(Lexeme_type::KEYWORD, "PRIMARY") ||
        match(Lexeme_type::KEYWORD, "FOREIGN") ||
        match(Lexeme_type::KEYWORD, "UNIQUE")) {
      table->m_constraints.push_back(parse_table_constraint());
      continue;
    }

    /* Parse column definition */
    table->m_columns.push_back(parse_column_definition());
  } while (match(Lexeme_type::OPERATOR, ","));

  if (!match(Lexeme_type::OPERATOR, ")")) {
    throw std::runtime_error("Expected closing parenthesis");
  }

  /* Parse table options */
  parse_table_options(table.get());

  return table;
}

std::unique_ptr<Column_def> Parser::parse_column_definition() {
  auto column = std::make_unique<Column_def>();
  const auto &state = m_state_stack.back();

  /* Parse column name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected column name");
  }
  column->m_name = state.m_lexeme.m_value;
  advance();

  /* Parse data type */
  column->m_type = parse_data_type();

  // Parse column constraints
  while (true) {
    if (match(Lexeme_type::KEYWORD, "NOT")) {
      if (!match(Lexeme_type::KEYWORD, "NULL")) {
        throw std::runtime_error("Expected NULL after NOT");
      }
      column->m_nullable = false;
    } else if (match(Lexeme_type::KEYWORD, "NULL")) {
      column->m_nullable = true;
    } else if (match(Lexeme_type::KEYWORD, "DEFAULT")) {
      column->m_default_value = parse_expression();
    } else if (match(Lexeme_type::KEYWORD, "PRIMARY")) {
      if (!match(Lexeme_type::KEYWORD, "KEY")) {
        throw std::runtime_error("Expected KEY after PRIMARY");
      }
      column->m_primary_key = true;
    } else if (match(Lexeme_type::KEYWORD, "UNIQUE")) {
      column->m_unique = true;
    } else if (match(Lexeme_type::KEYWORD, "CHECK")) {
      if (!match(Lexeme_type::OPERATOR, "(")) {
        throw std::runtime_error("Expected opening parenthesis");
      }
      column->m_check = parse_expression();
      if (!match(Lexeme_type::OPERATOR, ")")) {
        throw std::runtime_error("Expected closing parenthesis");
      }
    } else if (match(Lexeme_type::KEYWORD, "REFERENCES")) {
      column->m_references = parse_foreign_key_reference();
    } else if (match(Lexeme_type::KEYWORD, "AUTO_INCREMENT")) {
      column->m_auto_increment = true;
    } else if (match(Lexeme_type::KEYWORD, "COMMENT")) {
      if (state.m_lexeme.m_type != Lexeme_type::STRING_LITERAL) {
        throw std::runtime_error("Expected string literal for comment");
      }
      column->m_comment = state.m_lexeme.m_value;
      advance();
    } else {
      break;
    }
  }

  return column;
}

std::vector<std::string> Parser::parse_column_list_in_parentheses() {
  std::vector<std::string> columns;
  const auto &state = m_state_stack.back();

  /* Expect opening parenthesis */
  if (!match(Lexeme_type::OPERATOR, "(")) {
    throw std::runtime_error(
      "Expected opening parenthesis before column list at line " + 
      std::to_string(state.m_lexeme.m_line) + 
      ", column " + std::to_string(state.m_lexeme.m_col));
  }

  /* Parse comma-separated list of column names */
  do {
    /* Skip any whitespace */
    while (match(Lexeme_type::WHITESPACE)) {}

    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error(
        "Expected column name at line " + 
        std::to_string(state.m_lexeme.m_line) + 
        ", column " + std::to_string(state.m_lexeme.m_col));
    }

    /* Add column name to list */
    columns.push_back(state.m_lexeme.m_value);
    advance();

    /* Handle optional column length specification for indexes */
    if (match(Lexeme_type::OPERATOR, "(")) {
      /* Skip length specification as it's handled elsewhere */
      while (state.m_lexeme.m_type != Lexeme_type::OPERATOR || state.m_lexeme.m_value != ")") {
        advance();
      }
      if (!match(Lexeme_type::OPERATOR, ")")) {
        throw std::runtime_error(
          "Expected closing parenthesis after column length at line " + 
          std::to_string(state.m_lexeme.m_line) + 
          ", column " + std::to_string(state.m_lexeme.m_col));
      }
    }

    /* Skip any whitespace before comma or closing parenthesis */
    while (match(Lexeme_type::WHITESPACE)) {}

  } while (match(Lexeme_type::OPERATOR, ","));

  /* Expect closing parenthesis */
  if (!match(Lexeme_type::OPERATOR, ")")) {
    throw std::runtime_error(
      "Expected closing parenthesis after column list at line " + 
            std::to_string(state.m_lexeme.m_line) + 
            ", column " + std::to_string(state.m_lexeme.m_col));
  }

  return columns;
}

std::vector<Column_reference> Parser::parse_column_list_with_options() {
  std::vector<Column_reference> columns;
  const auto &state = m_state_stack.back();

  if (!match(Lexeme_type::OPERATOR, "(")) {
    throw std::runtime_error("Expected opening parenthesis before column list");
  }

  do {
    Column_reference col_ref;

    /* Handle schema-qualified column names */
    if (peek().m_type == Lexeme_type::OPERATOR && peek().m_value == ".") {
      col_ref.m_schema = state.m_lexeme.m_value;

      /* Skip schema name */
      advance();

      /* Skip dot */
      advance();
    }

    /* Parse table name if present (table.column notation) */
    if (peek().m_type == Lexeme_type::OPERATOR && peek().m_value == ".") {
      col_ref.m_table = state.m_lexeme.m_value;

      /* Skip table name */
      advance();

      /* Skip dot */
      advance();
    }

    /* Parse column name */
    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error("Expected column name");
    }
    col_ref.m_column = state.m_lexeme.m_value;
    advance();

    /* Parse optional column options */
    for (;;) {
      /* Skip whitespace */
      if (match(Lexeme_type::KEYWORD, "ASC")) {
        col_ref.m_ascending = true;
      } else if (match(Lexeme_type::KEYWORD, "DESC")) {
        col_ref.m_ascending = false;
      } else if (match(Lexeme_type::KEYWORD, "NULLS")) {
        if (match(Lexeme_type::KEYWORD, "FIRST")) {
          col_ref.m_nulls_position = Column_reference::Nulls_position::FIRST;
        } else if (match(Lexeme_type::KEYWORD, "LAST")) {
          col_ref.m_nulls_position = Column_reference::Nulls_position::LAST;
        } else {
          throw std::runtime_error("Expected FIRST or LAST after NULLS");
        }
      } else if (match(Lexeme_type::OPERATOR, "(")) {
        if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
          throw std::runtime_error("Expected number for column length");
        }
        col_ref.m_length = std::stoul(state.m_lexeme.m_value);
        advance();

        if (!match(Lexeme_type::OPERATOR, ")")) {
          throw std::runtime_error("Expected closing parenthesis after length");
        }
      } else if (match(Lexeme_type::KEYWORD, "COLLATE")) {
        /* Handle collation */
        if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
          throw std::runtime_error("Expected collation name");
        }
        col_ref.m_collation = state.m_lexeme.m_value;
        advance();
      } else {
        break;
      }
    }

    columns.push_back(std::move(col_ref));

  } while (match(Lexeme_type::OPERATOR, ","));

  if (!match(Lexeme_type::OPERATOR, ")")) {
    throw std::runtime_error("Expected closing parenthesis after column list");
  }

  return columns;
}

std::unique_ptr<Table_constraint> Parser::parse_table_constraint() {
  auto constraint = std::make_unique<Table_constraint>();
  const auto &state = m_state_stack.back();
  /* Parse optional constraint name */
  if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER) {
    constraint->m_name = state.m_lexeme.m_value;
    advance();
  }

  /* Parse constraint type */
  if (match(Lexeme_type::KEYWORD, "PRIMARY")) {
    if (!match(Lexeme_type::KEYWORD, "KEY")) {
      throw std::runtime_error("Expected KEY after PRIMARY");
    }
    constraint->m_type = Table_constraint::Type::primary_key;
    constraint->m_columns = parse_column_list_in_parentheses();
  } else if (match(Lexeme_type::KEYWORD, "FOREIGN")) {
    if (!match(Lexeme_type::KEYWORD, "KEY")) {
      throw std::runtime_error("Expected KEY after FOREIGN");
    }
    constraint->m_type = Table_constraint::Type::foreign_key;
    constraint->m_columns = parse_column_list_in_parentheses();
    if (!match(Lexeme_type::KEYWORD, "REFERENCES")) {
      throw std::runtime_error("Expected REFERENCES");
    }
    constraint->m_reference = parse_foreign_key_reference();
  } else if (match(Lexeme_type::KEYWORD, "UNIQUE")) {
    constraint->m_type = Table_constraint::Type::unique;
    constraint->m_columns = parse_column_list_in_parentheses();
  } else if (match(Lexeme_type::KEYWORD, "CHECK")) {
    constraint->m_type = Table_constraint::Type::check;
    if (!match(Lexeme_type::OPERATOR, "(")) {
      throw std::runtime_error("Expected opening parenthesis");
    }
    constraint->m_check = parse_expression();
    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis");
    }
  } else {
    throw std::runtime_error("Unknown constraint type");
  }

  return constraint;
}

std::unique_ptr<Create_index_def> Parser::parse_create_index() {
  auto index = std::make_unique<Create_index_def>();
  const auto &state = m_state_stack.back();

  /* Parse optional UNIQUE */
  if (match(Lexeme_type::KEYWORD, "UNIQUE")) {
    index->m_unique = true;
  }

  /* Parse index name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected index name");
  }
  index->m_index_name = state.m_lexeme.m_value;
  advance();

  /* Parse ON */
  if (!match(Lexeme_type::KEYWORD, "ON")) {
    throw std::runtime_error("Expected ON");
  }

  /* Parse table name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected table name");
  }
  index->m_table_name = state.m_lexeme.m_value;
  advance();

  /* Parse column list */
  if (!match(Lexeme_type::OPERATOR, "(")) {
    throw std::runtime_error("Expected opening parenthesis");
  }

  do {
    Index_column col;

    /* Parse column expression or name */
    if (match(Lexeme_type::OPERATOR, "(")) {
      col.m_expression = parse_expression();
      if (!match(Lexeme_type::OPERATOR, ")")) {
        throw std::runtime_error("Expected closing parenthesis");
      }
    } else {
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected column name");
      }
      col.m_column_name = state.m_lexeme.m_value;
      advance();
    }

    /* Parse optional length for string columns */
    if (match(Lexeme_type::OPERATOR, "(")) {
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected number");
      }
      col.m_length = std::stoul(state.m_lexeme.m_value);
      advance();
      if (!match(Lexeme_type::OPERATOR, ")")) {
        throw std::runtime_error("Expected closing parenthesis");
      }
    }

    /* Parse optional sort direction */
    if (match(Lexeme_type::KEYWORD, "ASC")) {
      col.m_ascending = true;
    } else if (match(Lexeme_type::KEYWORD, "DESC")) {
      col.m_ascending = false;
    }

    index->m_columns.push_back(std::move(col));
  } while (match(Lexeme_type::OPERATOR, ","));

  if (!match(Lexeme_type::OPERATOR, ")")) {
    throw std::runtime_error("Expected closing parenthesis");
  }

  // Parse optional index type
  if (match(Lexeme_type::KEYWORD, "USING")) {
    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error("Expected index type");
    }
    index->m_index_type = state.m_lexeme.m_value;
    advance();
  }

  return index;
}

std::unique_ptr<Create_view_def> Parser::parse_create_view() {
  auto view = std::make_unique<Create_view_def>();
  const auto &state = m_state_stack.back();
  /* Parse optional OR REPLACE */
  if (match(Lexeme_type::KEYWORD, "OR")) {
    if (!match(Lexeme_type::KEYWORD, "REPLACE")) {
      throw std::runtime_error("Expected REPLACE after OR");
    }
    view->m_or_replace = true;
  }

  /* Parse view name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected view name");
  }
  view->m_view_name = state.m_lexeme.m_value;
  advance();

  /* Parse optional column list */
  if (match(Lexeme_type::OPERATOR, "(")) {
    do {
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected column name");
      }
      view->m_column_names.push_back(state.m_lexeme.m_value);
      advance();
    } while (match(Lexeme_type::OPERATOR, ","));

    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis");
    }
  }

  /* Parse AS keyword */
  if (!match(Lexeme_type::KEYWORD, "AS")) {
    throw std::runtime_error("Expected AS");
  }

  /* Parse SELECT statement */
  view->m_select = parse_select_statement();

  /* Parse optional WITH CHECK OPTION */
  if (match(Lexeme_type::KEYWORD, "WITH")) {
    if (!match(Lexeme_type::KEYWORD, "CHECK")) {
      throw std::runtime_error("Expected CHECK after WITH");
    }
    if (!match(Lexeme_type::KEYWORD, "OPTION")) {
      throw std::runtime_error("Expected OPTION after CHECK");
    }
    view->m_with_check_option = true;
  }

  return view;
}

std::unique_ptr<Function_call> Parser::parse_function_call() {
  auto func = std::make_unique<Function_call>();
  const auto &state = m_state_stack.back();

  /* Parse function name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error(
      "Expected function name at line " + 
      std::to_string(state.m_lexeme.m_line) + 
      ", column " + std::to_string(state.m_lexeme.m_col));
  }
  func->m_function_name = state.m_lexeme.m_value;
  advance();

  /* Handle opening parenthesis */
  if (!match(Lexeme_type::OPERATOR, "(")) {
    throw std::runtime_error(
      "Expected opening parenthesis after function name '" + 
      func->m_function_name + "'");
  }

  /* Special handling for COUNT(*) */
  if (func->m_function_name == "COUNT" && match(Lexeme_type::OPERATOR, "*")) {
    func->m_star = true;
    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis after COUNT(*)");
    }
    return parse_optional_over_clause(std::move(func));
  }

  /* Handle DISTINCT if present */
  if (match(Lexeme_type::KEYWORD, "DISTINCT")) {
    func->m_distinct = true;
  }

  /* Parse argument list */
  if (!match(Lexeme_type::OPERATOR, ")")) {  // Empty argument list check
    do {
      func->m_arguments.push_back(parse_expression());
    } while (match(Lexeme_type::OPERATOR, ","));

    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error(
        "Expected closing parenthesis after function arguments in '" + 
        func->m_function_name + "'");
      }
  }

  return parse_optional_over_clause(std::move(func));
}

std::unique_ptr<Function_call> Parser::parse_optional_over_clause(std::unique_ptr<Function_call> func) {
    
  /* Check for OVER clause (window functions) */
  if (match(Lexeme_type::KEYWORD, "OVER")) {
    func->m_window = parse_window_specification();
  }
    
  return func;
}

std::unique_ptr<Window_spec> Parser::parse_window_specification() {
  auto window = std::make_unique<Window_spec>();
  const auto &state = m_state_stack.back();

  /* Handle named window reference */
  if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER && !peek_is_operator("(")) {
    window->m_reference_name = state.m_lexeme.m_value;
    advance();
    return window;
  }

  /* Handle opening parenthesis */
  if (!match(Lexeme_type::OPERATOR, "(")) {
    throw std::runtime_error("Expected opening parenthesis after OVER");
  }

  /* Parse window definition */
  for (;;) {
    if (match(Lexeme_type::KEYWORD, "PARTITION")) {
      if (!match(Lexeme_type::KEYWORD, "BY")) {
        throw std::runtime_error("Expected BY after PARTITION");
      }
      window->m_partition = parse_partition_by();
    } else if (match(Lexeme_type::KEYWORD, "ORDER")) {
      if (!match(Lexeme_type::KEYWORD, "BY")) {
        throw std::runtime_error("Expected BY after ORDER");
      }
      window->m_order = parse_order_by();
    } else if (match(Lexeme_type::KEYWORD, "ROWS") || match(Lexeme_type::KEYWORD, "RANGE") || match(Lexeme_type::KEYWORD, "GROUPS")) {
      window->m_frame = parse_frame_clause(
        state.m_prev_lexeme.m_value == "ROWS" ? Window_spec::Frame::Type::rows :
        state.m_prev_lexeme.m_value == "RANGE" ? Window_spec::Frame::Type::range : Window_spec::Frame::Type::groups);
    } else {
      break;
    }

    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis in window specification");
    }
  }

  return window;
}

Window_spec::Partition Parser::parse_partition_by() {
  Window_spec::Partition partition;
    
  do {
    partition.m_columns.push_back(parse_column_ref());
  } while (match(Lexeme_type::OPERATOR, ","));
    
  return partition;
}

std::unique_ptr<Window_spec::Frame> Parser::parse_frame_clause(
  Window_spec::Frame::Type frame_type) {
    
  auto frame = std::make_unique<Window_spec::Frame>();
  frame->m_type = frame_type;

  /* Parse frame start */
  if (match(Lexeme_type::KEYWORD, "BETWEEN")) {
    frame->m_start = parse_frame_bound();
        
    if (!match(Lexeme_type::KEYWORD, "AND")) {
      throw std::runtime_error("Expected AND in frame clause");
    }
        
    frame->m_end = parse_frame_bound();
  } else {
    frame->m_start = parse_frame_bound();
    /* Single bound means start = end */
    frame->m_end = std::move(frame->m_start);
  }

  /* Parse EXCLUDE clause if present */
  if (match(Lexeme_type::KEYWORD, "EXCLUDE")) {
    if (match(Lexeme_type::KEYWORD, "CURRENT")) {
      if (!match(Lexeme_type::KEYWORD, "ROW")) {
        throw std::runtime_error("Expected ROW after CURRENT");
      }
      frame->m_exclude = Window_spec::Frame::Exclude::current_row;
    } else if (match(Lexeme_type::KEYWORD, "GROUP")) {
      frame->m_exclude = Window_spec::Frame::Exclude::group;
    } else if (match(Lexeme_type::KEYWORD, "TIES")) {
      frame->m_exclude = Window_spec::Frame::Exclude::ties;
    } else if (match(Lexeme_type::KEYWORD, "NO")) {
      if (!match(Lexeme_type::KEYWORD, "OTHERS")) {
        throw std::runtime_error("Expected OTHERS after NO");
      }
      frame->m_exclude = Window_spec::Frame::Exclude::no_others;
    } else {
      throw std::runtime_error("Invalid EXCLUDE clause in frame specification");
    }
  }

  return frame;
}

Window_spec::Frame::Bound Parser::parse_frame_bound() {
  Window_spec::Frame::Bound bound;

  if (match(Lexeme_type::KEYWORD, "CURRENT")) {
    if (!match(Lexeme_type::KEYWORD, "ROW")) {
      throw std::runtime_error("Expected ROW after CURRENT");
    }
    bound.m_type = Window_spec::Frame::Bound::Type::current_row;
  } else if (match(Lexeme_type::KEYWORD, "UNBOUNDED")) {
    if (match(Lexeme_type::KEYWORD, "PRECEDING")) {
      bound.m_type = Window_spec::Frame::Bound::Type::unbounded_preceding;
    } else if (match(Lexeme_type::KEYWORD, "FOLLOWING")) {
      bound.m_type = Window_spec::Frame::Bound::Type::unbounded_following;
    } else {
      throw std::runtime_error("Expected PRECEDING or FOLLOWING after UNBOUNDED");
    }
  } else {
    /* Parse numeric offset */
    auto offset = parse_expression();
        
    if (match(Lexeme_type::KEYWORD, "PRECEDING")) {
      bound.m_type = Window_spec::Frame::Bound::Type::preceding;
    } else if (match(Lexeme_type::KEYWORD, "FOLLOWING")) {
      bound.m_type = Window_spec::Frame::Bound::Type::following;
    } else {
      throw std::runtime_error("Expected PRECEDING or FOLLOWING");
    }
        
    bound.m_offset = std::move(offset);
  }

  return bound;
}

void Parser::parse_table_options(Create_table_def* table) {
  const auto &state = m_state_stack.back();

  for (;;) {
    if (match(Lexeme_type::KEYWORD, "ENGINE")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after ENGINE");
      }
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected engine name");
      }
      table->m_options.m_engine = state.m_lexeme.m_value;
      advance();
    } else if (match(Lexeme_type::KEYWORD, "AUTO_INCREMENT")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after AUTO_INCREMENT");
      }
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected number for AUTO_INCREMENT");
      }
      table->m_options.m_auto_increment = std::stoull(state.m_lexeme.m_value);
      advance();
    } else if (match(Lexeme_type::KEYWORD, "CHARACTER")) {
      if (!match(Lexeme_type::KEYWORD, "SET")) {
        throw std::runtime_error("Expected SET after CHARACTER");
      }
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after CHARACTER SET");
      }
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected character set name");
      }
      table->m_options.m_charset = state.m_lexeme.m_value;
      advance();
    } else if (match(Lexeme_type::KEYWORD, "CHARSET")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after CHARSET");
      }
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected character set name");
      }
      table->m_options.m_charset = state.m_lexeme.m_value;
      advance();
    } else if (match(Lexeme_type::KEYWORD, "COLLATE")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after COLLATE");
      }
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected collation name");
      }
      table->m_options.m_collate = state.m_lexeme.m_value;
      advance();
    } else if (match(Lexeme_type::KEYWORD, "COMMENT")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after COMMENT");
      }
      if (state.m_lexeme.m_type != Lexeme_type::STRING_LITERAL) {
        throw std::runtime_error("Expected string literal for COMMENT");
      }
      table->m_options.m_comment = state.m_lexeme.m_value;
      advance();
    } else if (match(Lexeme_type::KEYWORD, "ROW_FORMAT")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after ROW_FORMAT");
      }
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected ROW_FORMAT value");
      }
      table->m_options.m_row_format = state.m_lexeme.m_value;
      advance();
    } else if (match(Lexeme_type::KEYWORD, "KEY_BLOCK_SIZE")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after KEY_BLOCK_SIZE");
      }
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected number for KEY_BLOCK_SIZE");
      }
      table->m_options.m_key_block_size = std::stoull(state.m_lexeme.m_value);
      advance();
    } else if (match(Lexeme_type::KEYWORD, "MAX_ROWS")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after MAX_ROWS");
      }
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected number for MAX_ROWS");
      }
      table->m_options.m_max_rows = std::stoull(state.m_lexeme.m_value);
      advance();
    } else if (match(Lexeme_type::KEYWORD, "MIN_ROWS")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after MIN_ROWS");
      }
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected number for MIN_ROWS");
      }
      table->m_options.m_min_rows = std::stoull(state.m_lexeme.m_value);
      advance();
    } else if (match(Lexeme_type::KEYWORD, "TABLESPACE")) {
      if (!match(Lexeme_type::OPERATOR, "=") && !is_whitespace_before(state.m_lexeme)) {
        throw std::runtime_error("Expected = after TABLESPACE");
      }
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected tablespace name");
      }
      table->m_options.m_tablespace = state.m_lexeme.m_value;
      advance();
    } else {
      /* No more table options */
      break;
    }

    /* Handle optional comma between options */
    (void) match(Lexeme_type::OPERATOR, ",");
  }
}

Data_type Parser::parse_data_type() {
  const auto &state = m_state_stack.back();

  /* Parse basic type name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected data type name");
  }

  std::string type_name = to_upper(state.m_lexeme.m_value);
  advance();

  Data_type type;

  /* Parse the type */
  if (type_name == "INT" || type_name == "INTEGER") {
    type.m_base_type = Data_type::Type::integer;
  } else if (type_name == "BIGINT") {
    type.m_base_type = Data_type::Type::bigint;
  } else if (type_name == "SMALLINT") {
    type.m_base_type = Data_type::Type::smallint;
  } else if (type_name == "DECIMAL" || type_name == "NUMERIC") {
    type.m_base_type = type_name == "DECIMAL" ? Data_type::Type::decimal : Data_type::Type::numeric;

    /* Parse optional precision and scale */
    if (match(Lexeme_type::OPERATOR, "(")) {
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected precision value");
      }
      type.m_precision = std::stoull(state.m_lexeme.m_value);
      advance();

      if (match(Lexeme_type::OPERATOR, ",")) {
        if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
          throw std::runtime_error("Expected scale value");
        }
        type.m_scale = std::stoull(state.m_lexeme.m_value);
        advance();
      }

      if (!match(Lexeme_type::OPERATOR, ")")) {
        throw std::runtime_error("Expected closing parenthesis");
      }
    }
  } else if (type_name == "FLOAT") {
    type.m_base_type = Data_type::Type::float_;
  } else if (type_name == "DOUBLE" || (type_name == "DOUBLE" && match(Lexeme_type::KEYWORD, "PRECISION"))) {
    type.m_base_type = Data_type::Type::double_;
  } else if (type_name == "CHAR" || type_name == "VARCHAR") {
    type.m_base_type = type_name == "CHAR" ? Data_type::Type::char_ : Data_type::Type::varchar;

    /* Parse required length for VARCHAR, optional for CHAR */
    if (match(Lexeme_type::OPERATOR, "(")) {
      if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
        throw std::runtime_error("Expected length value");
      }
      type.m_length = std::stoull(state.m_lexeme.m_value);
      advance();

      if (!match(Lexeme_type::OPERATOR, ")")) {
        throw std::runtime_error("Expected closing parenthesis");
      }
    } else if (type.m_base_type == Data_type::Type::varchar) {
      throw std::runtime_error("VARCHAR requires length specification");
    } else if (type_name == "TEXT") {
      type.m_base_type = Data_type::Type::text;
    } else if (type_name == "DATE") {
      type.m_base_type = Data_type::Type::date;
    } else if (type_name == "TIME") {
      type.m_base_type = Data_type::Type::time;
    } else if (type_name == "TIMESTAMP") {
      type.m_base_type = Data_type::Type::timestamp;
    } else if (type_name == "BOOLEAN" || type_name == "BOOL") {
      type.m_base_type = Data_type::Type::boolean;
    } else if (type_name == "BLOB") {
      type.m_base_type = Data_type::Type::blob;
    } else if (type_name == "JSON") {
      type.m_base_type = Data_type::Type::json;
    } else {
      throw std::runtime_error("Unknown data type: " + type_name);
    }

    /* Parse optional character set and collation */
    for (;;) {
      if (match(Lexeme_type::KEYWORD, "CHARACTER")) {
        if (!match(Lexeme_type::KEYWORD, "SET")) {
          throw std::runtime_error("Expected SET after CHARACTER");
        }
      } else if (match(Lexeme_type::KEYWORD, "CHARSET")) {
        if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
          throw std::runtime_error("Expected character set name");
        }
        type.m_charset = state.m_lexeme.m_value;
        advance();
      } else if (match(Lexeme_type::KEYWORD, "COLLATE")) {
        if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
          throw std::runtime_error("Expected collation name");
        }
        type.m_collation = state.m_lexeme.m_value;
        advance();
      } else {
        break;
      }
    }
  }

  return type;
}

std::unique_ptr<Foreign_key_reference> Parser::parse_foreign_key_reference() {
  auto ref = std::make_unique<Foreign_key_reference>();
  const auto &state = m_state_stack.back();

  /* Parse REFERENCES keyword if not already consumed */
  if (!state.m_prev_lexeme.m_value.empty() && 
      to_upper(state.m_prev_lexeme.m_value) != "REFERENCES") {
    if (!match(Lexeme_type::KEYWORD, "REFERENCES")) {
      throw std::runtime_error("Expected REFERENCES keyword");
    }
  }

  /* Parse referenced table name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error(
      "Expected table name at line " + 
      std::to_string(state.m_lexeme.m_line) + 
      ", column " + std::to_string(state.m_lexeme.m_col));
  }

  ref->m_table_name = state.m_lexeme.m_value;
  advance();

  /* Parse column list */
  if (match(Lexeme_type::OPERATOR, "(")) {

    do {
      if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
        throw std::runtime_error("Expected column name at line " + std::to_string(state.m_lexeme.m_line) + ", column " + std::to_string(state.m_lexeme.m_col));
      }
      ref->m_column_names.push_back(state.m_lexeme.m_value);
      advance();
    } while (match(Lexeme_type::OPERATOR, ","));

    if (!match(Lexeme_type::OPERATOR, ")")) {
      throw std::runtime_error("Expected closing parenthesis after column list");
    }
  }

  /* Parse optional MATCH clause */
  if (match(Lexeme_type::KEYWORD, "MATCH")) {
    if (match(Lexeme_type::KEYWORD, "FULL")) {
      ref->m_match = Foreign_key_reference::Match_type::full;
    } else if (match(Lexeme_type::KEYWORD, "PARTIAL")) {
      ref->m_match = Foreign_key_reference::Match_type::partial;
    } else if (match(Lexeme_type::KEYWORD, "SIMPLE")) {
      ref->m_match = Foreign_key_reference::Match_type::simple;
    } else {
      throw std::runtime_error("Expected FULL, PARTIAL or SIMPLE after MATCH at line " + std::to_string(state.m_lexeme.m_line));
    }
  }

  /* Parse optional ON DELETE clause */
  if (match(Lexeme_type::KEYWORD, "ON")) {
    if (match(Lexeme_type::KEYWORD, "DELETE")) {
      ref->m_on_delete = parse_reference_option();
    } else {
      /* Put back the ON token as it might be for ON UPDATE */
      backup();
    }
  }

  /* Parse optional ON UPDATE clause */
  if (match(Lexeme_type::KEYWORD, "ON")) {
    if (match(Lexeme_type::KEYWORD, "UPDATE")) {
      ref->m_on_update = parse_reference_option();
    } else {
      throw std::runtime_error("Expected UPDATE after ON at line " + std::to_string(state.m_lexeme.m_line));
    }
  }

  /* Parse optional ENFORCED clause */
  if (match(Lexeme_type::KEYWORD, "ENFORCED")) {
    ref->m_enforced = true;
  } else if (match(Lexeme_type::KEYWORD, "NOT")) {
    if (!match(Lexeme_type::KEYWORD, "ENFORCED")) {
      throw std::runtime_error("Expected ENFORCED after NOT at line " + std::to_string(state.m_lexeme.m_line));
    }
    ref->m_enforced = false;
  }

  return ref;
}

Foreign_key_reference::Action Parser::parse_reference_option() {
  const auto &state = m_state_stack.back();

  if (match(Lexeme_type::KEYWORD, "RESTRICT")) {
    return Foreign_key_reference::Action::restrict;
  } else if (match(Lexeme_type::KEYWORD, "CASCADE")) {
    return Foreign_key_reference::Action::cascade;
  } else if (match(Lexeme_type::KEYWORD, "SET")) {
    if (match(Lexeme_type::KEYWORD, "NULL")) {
      return Foreign_key_reference::Action::set_null;
    } else if (match(Lexeme_type::KEYWORD, "DEFAULT")) {
      return Foreign_key_reference::Action::set_default;
    } else {
      throw std::runtime_error(
        "Expected NULL or DEFAULT after SET at line " + 
        std::to_string(state.m_lexeme.m_line));
    }
  } else if (match(Lexeme_type::KEYWORD, "NO")) {
    if (!match(Lexeme_type::KEYWORD, "ACTION")) {
      throw std::runtime_error(
        "Expected ACTION after NO at line " + 
        std::to_string(state.m_lexeme.m_line));
    }
    return Foreign_key_reference::Action::no_action;
  } else {
    throw std::runtime_error(
      "Expected RESTRICT, CASCADE, SET NULL, SET DEFAULT, or NO ACTION at line " + 
      std::to_string(state.m_lexeme.m_line));
  }
}

std::unique_ptr<Ast_base> Parser::parse_column_expression() {
  std::unique_ptr<Ast_base> expr;

  const auto &state = m_state_stack.back();

  /* Check for function call */
  if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER && peek().m_type == Lexeme_type::OPERATOR && peek().m_value == "(") {
    expr = parse_function_call();
  } else if (match(Lexeme_type::KEYWORD, "CASE")) {
    /* Check for case expression */
    throw std::runtime_error("Case expressions are not supported");
  } else {
    /* Regular column reference or expression */
    expr = parse_expression();
  }

  /* Handle column alias */
  if (match(Lexeme_type::KEYWORD, "AS")) {
    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error("Expected identifier after AS");
    }
    if (auto* col = dynamic_cast<Column_ref*>(expr.get())) {
      col->m_alias = state.m_lexeme.m_value;
      advance();
    } else {
      throw std::runtime_error("Alias can only be applied to column references");
    }
  }

  return expr;
}

std::vector<std::unique_ptr<Ast_base>> Parser::parse_column_list() {
  std::vector<std::unique_ptr<Ast_base>> columns;

  /* Handle SELECT * */
  if (match(Lexeme_type::OPERATOR, "*")) {
    auto col = std::make_unique<Column_ref>();
    col->m_column_name = "*";
    columns.emplace_back(std::move(col));
    return columns;
  }

  /* Parse column expressions */
  do {
    columns.emplace_back(parse_column_expression());
  } while (match(Lexeme_type::OPERATOR, ","));

  return columns;
}

std::unique_ptr<Alter_table_stmt> Parser::parse_alter_table() {
  auto alter = std::make_unique<Alter_table_stmt>();
  const auto &state = m_state_stack.back();

  /* Parse optional IF EXISTS */
  if (match(Lexeme_type::KEYWORD, "IF")) {
    if (!match(Lexeme_type::KEYWORD, "EXISTS")) {
      throw std::runtime_error("Expected EXISTS after IF");
    }
    alter->m_if_exists = true;
  }

  /* Parse ONLY keyword for inheritance */
  if (match(Lexeme_type::KEYWORD, "ONLY")) {
    alter->m_only = true;
  }

  /* Parse table name */
  if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
    throw std::runtime_error("Expected table name");
  }
  alter->m_table_name = state.m_lexeme.m_value;
  advance();

  /* Parse optional * for inheritance */
  if (match(Lexeme_type::OPERATOR, "*")) {
    if (alter->m_only) {
      throw std::runtime_error("Cannot specify both ONLY and *");
    }
    alter->m_all_inheritance = true;
  }

#if 0
  /* Parse alterations */
  do {
    if (match(Lexeme_type::KEYWORD, "ADD")) {
      alter->m_alterations.push_back(parse_add_action());
    } else if (match(Lexeme_type::KEYWORD, "DROP")) {
      alter->m_alterations.push_back(parse_drop_action());
    } else if (match(Lexeme_type::KEYWORD, "ALTER")) {
      alter->m_alterations.push_back(parse_alter_action());
    } else if (match(Lexeme_type::KEYWORD, "MODIFY")) {
      alter->m_alterations.push_back(parse_modify_action());
    } else if (match(Lexeme_type::KEYWORD, "RENAME")) {
      alter->m_alterations.push_back(parse_rename_action());
    } else if (match(Lexeme_type::KEYWORD, "SET")) {
      alter->m_alterations.push_back(parse_set_action());
    } else if (match(Lexeme_type::KEYWORD, "RESET")) {
      alter->m_alterations.push_back(parse_reset_action());
    } else if (match(Lexeme_type::KEYWORD, "CLUSTER")) {
      alter->m_alterations.push_back(parse_cluster_action());
    } else if (match(Lexeme_type::KEYWORD, "ENABLE")) {
      alter->m_alterations.push_back(parse_enable_action());
    } else if (match(Lexeme_type::KEYWORD, "DISABLE")) {
      alter->m_alterations.push_back(parse_disable_action());
    } else if (match(Lexeme_type::KEYWORD, "ATTACH")) {
      alter->m_alterations.push_back(parse_attach_partition());
    } else if (match(Lexeme_type::KEYWORD, "DETACH")) {
      alter->m_alterations.push_back(parse_detach_partition());
    } else {
      throw std::runtime_error("Unknown ALTER TABLE action");
    }
  } while (match(Lexeme_type::OPERATOR, ","));
  #endif

  return alter;
}

std::unique_ptr<Literal> Parser::parse_literal() {
  auto literal = std::make_unique<Literal>();
  const auto &state = m_state_stack.back();
  switch (state.m_lexeme.m_type) {
    case Lexeme_type::NUMBER:
      /* Determine if it's integer or floating point */
      if (state.m_lexeme.m_value.find('.') != std::string::npos) {
        literal->m_type = Literal::Type::floating;
      } else {
        literal->m_type = Literal::Type::integer;
      }
      literal->m_value = state.m_lexeme.m_value;
      advance();
      break;

    case Lexeme_type::STRING_LITERAL:
      literal->m_type = Literal::Type::string;
      literal->m_value = state.m_lexeme.m_value;
      advance();
      break;

    case Lexeme_type::KEYWORD:
      if (state.m_lexeme.m_value == "NULL") {
        literal->m_type = Literal::Type::null;
        literal->m_value = "NULL";
        advance();
      } else if (state.m_lexeme.m_value == "TRUE" ||
        state.m_lexeme.m_value == "FALSE") {
        literal->m_type = Literal::Type::boolean;
        literal->m_value = state.m_lexeme.m_value;
        advance();
      } else {
        throw std::runtime_error("Unexpected keyword in literal context");
      }
      break;

    default:
      throw std::runtime_error("Expected literal value");
  }

  return literal;
}

std::unique_ptr<Ast_base> Parser::parse_primary() {
  /* Handle parentheses */
  if (match(Lexeme_type::OPERATOR, "(")) {
    auto expr = parse_expression();
    expect(Lexeme_type::OPERATOR, ")");
    return expr;
  }

  const auto &state = m_state_stack.back();

  /* Handle function calls */
  if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER && peek(1).m_type == Lexeme_type::OPERATOR && peek(1).m_value == "(") {
    return parse_function_call();
  }

  /* Handle column references */
  if (state.m_lexeme.m_type == Lexeme_type::IDENTIFIER) {
    return parse_column_ref();
  }

  /* Handle literals */
  return parse_literal();
}

std::unique_ptr<Binary_op> Parser::parse_factor() {
  auto left = parse_primary();

  while (auto op_type = parse_operator()) {
    auto op = std::make_unique<Binary_op>();
    op->m_op = op_type.value();
    advance();

    op->m_left = std::move(left);
    op->m_right = parse_primary();
    left = std::move(op);
  }

  return std::unique_ptr<Binary_op>(dynamic_cast<Binary_op*>(left.release()));
}

std::unique_ptr<Binary_op> Parser::parse_term() {
  auto left = parse_factor();

  while (auto op_type = parse_operator()) {
    auto op = std::make_unique<Binary_op>();

    op->m_op = op_type.value();

    advance();

    op->m_left = std::move(left);
    op->m_right = parse_factor();
    left = std::move(op);
  }

  return left;
}

std::optional<size_t> Parser::parse_limit() {
  auto &state = m_state_stack.back();

  if (state.m_lexeme.m_type != Lexeme_type::NUMBER) {
    throw std::runtime_error("Expected number after LIMIT");
  }
    
  size_t limit = std::stoull(state.m_lexeme.m_value);
  advance();
    
  return limit;
}

std::vector<std::unique_ptr<Order_by_item>> Parser::parse_order_by() {
  std::vector<std::unique_ptr<Order_by_item>> items;

  do {
    auto item = std::make_unique<Order_by_item>();

    item->m_column = parse_column_ref();

    /* Parse optional ASC/DESC */
    if (match(Lexeme_type::KEYWORD, "DESC")) {
      item->m_ascending = false;
    } else {
      /* ASC is optional */
      (void) match(Lexeme_type::KEYWORD, "ASC");
    }

    /* Parse optional NULLS FIRST/LAST */
    if (match(Lexeme_type::KEYWORD, "NULLS")) {
      if (match(Lexeme_type::KEYWORD, "FIRST")) {
        item->m_nulls = "FIRST";
      } else if (match(Lexeme_type::KEYWORD, "LAST")) {
        item->m_nulls = "LAST";
      } else {
        throw std::runtime_error("Expected FIRST or LAST after NULLS");
      }
    }

    items.push_back(std::move(item));

  } while (match(Lexeme_type::OPERATOR, ","));

  return items;
}

std::unique_ptr<Select_stmt> Parser::parse_select_statement() {
  auto select = std::make_unique<Select_stmt>();

  /* Parse DISTINCT if present */
  if (match(Lexeme_type::KEYWORD, "DISTINCT")) {
    select->m_distinct = true;
  }

  /* Parse column list */
  select->m_columns = parse_column_list();

  /* Parse FROM clause */
  if (!match(Lexeme_type::KEYWORD, "FROM")) {
    throw std::runtime_error("Expected FROM clause");
  }

  select->m_from = parse_table_references();

  /* Parse optional clauses */
  for (;;) {
    if (match(Lexeme_type::KEYWORD, "WHERE")) {
      if (select->m_where) {
        throw std::runtime_error("Duplicate WHERE clause");
      }
      select->m_where = parse_where_clause();
    } else if (match(Lexeme_type::KEYWORD, "GROUP")) {
      if (select->m_group_by) {
        throw std::runtime_error("Duplicate GROUP BY clause");
      }
      if (!match(Lexeme_type::KEYWORD, "BY")) {
        throw std::runtime_error("Expected BY after GROUP");
      }
      select->m_group_by = parse_group_by();
    } else if (match(Lexeme_type::KEYWORD, "HAVING")) {
      if (!select->m_group_by) {
        throw std::runtime_error("HAVING clause without GROUP BY");
      }
      if (select->m_group_by->m_having) {
        throw std::runtime_error("Duplicate HAVING clause");
      }
      select->m_group_by->m_having = parse_expression();
    } else if (match(Lexeme_type::KEYWORD, "ORDER")) {
      if (!select->m_order_by.empty()) {
        throw std::runtime_error("Duplicate ORDER BY clause");
      }
      if (!match(Lexeme_type::KEYWORD, "BY")) {
        throw std::runtime_error("Expected BY after ORDER");
      }
      select->m_order_by = parse_order_by();
    } else if (match(Lexeme_type::KEYWORD, "LIMIT")) {
      if (select->m_limit) {
        throw std::runtime_error("Duplicate LIMIT clause");
      }
      select->m_limit = parse_limit();
    } else {
      break;
    }
  }

  return select;
}

std::unique_ptr<Alter_stmt> Parser::parse_alter_statement() {
  throw std::runtime_error("ALTER not implemented");
}

std::unique_ptr<Drop_stmt> Parser::parse_drop_statement() {
  auto drop = std::make_unique<Drop_stmt>();

  /* Parse optional IF EXISTS */
  if (match(Lexeme_type::KEYWORD, "IF")) {
    if (!match(Lexeme_type::KEYWORD, "EXISTS")) {
      throw std::runtime_error("Expected EXISTS after IF");
    }
    drop->m_if_exists = true;
  }

  /* Parse object type */
  if (match(Lexeme_type::KEYWORD, "TABLE")) {
    drop->m_object_type = Drop_stmt::Object_type::table;
  } else if (match(Lexeme_type::KEYWORD, "INDEX")) {
    drop->m_object_type = Drop_stmt::Object_type::index;
  } else if (match(Lexeme_type::KEYWORD, "VIEW")) {
    drop->m_object_type = Drop_stmt::Object_type::view;
  } else if (match(Lexeme_type::KEYWORD, "TRIGGER")) {
    drop->m_object_type = Drop_stmt::Object_type::trigger;
  } else if (match(Lexeme_type::KEYWORD, "SEQUENCE")) {
    // TODO: Implement DROP SEQUENCE
    throw std::runtime_error("DROP SEQUENCE not implemented");
  } else {
    throw std::runtime_error("Unknown object type for DROP");
  }

  /* Parse object names */
  do {
    const auto &state = m_state_stack.back();

    if (state.m_lexeme.m_type != Lexeme_type::IDENTIFIER) {
      throw std::runtime_error("Expected object name");
    }
    drop->m_object_names.push_back(state.m_lexeme.m_value);
    advance();
  } while (match(Lexeme_type::OPERATOR, ","));

  /* Parse optional CASCADE/RESTRICT */
  if (match(Lexeme_type::KEYWORD, "CASCADE")) {
    drop->m_cascade = true;
  } else if (match(Lexeme_type::KEYWORD, "RESTRICT")) {
    drop->m_cascade = false;
  }

  return drop;
}

Ast_node Parser::parse_statement() {
  /* Handle different statement types */
  if (match(Lexeme_type::KEYWORD, "SELECT")) {
    return parse_select_statement();
  } else if (match(Lexeme_type::KEYWORD, "INSERT")) {
    return parse_insert_statement();
  } else if (match(Lexeme_type::KEYWORD, "UPDATE")) {
    return parse_update_statement();
  } else if (match(Lexeme_type::KEYWORD, "DELETE")) {
    return parse_delete_statement();
  } else if (match(Lexeme_type::KEYWORD, "CREATE")) {
    return parse_create_statement();
  } else if (match(Lexeme_type::KEYWORD, "DROP")) {
    return parse_drop_statement();
  } else if (match(Lexeme_type::KEYWORD, "ALTER")) {
    // FIXME: Implement ALTER 
    return parse_alter_statement();
  } else {
    auto &state = m_state_stack.back();
    throw std::runtime_error("Unexpected token at start of statement: " + state.m_lexeme.m_value);
  }
}

Ast_node Parser::parse() { 
  return parse_statement();
}

} // namespace sql