/***********************************************************************
Copyright 2024 Sunny Bains

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or Implied.
See the License for the specific language governing permissions and
limitations under the License.

***********************************************************************/

#pragma once

#include <algorithm>
#include <format>
#include <optional>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <stdexcept>
#include <vector>
#include <variant>

#include <cassert>

#include "sql/ast.h"

namespace sql {

using Ast_node = std::variant<
  std::unique_ptr<Alter_stmt>,
  std::unique_ptr<Column_ref>,
  std::unique_ptr<Create_table_stmt>,
  std::unique_ptr<Create_trigger_stmt>,
  std::unique_ptr<Create_view_stmt>,
  std::unique_ptr<Drop_stmt>,
  std::unique_ptr<Grant_revoke_stmt>,
  std::unique_ptr<Merge_stmt>,
  std::unique_ptr<Select_stmt>,
  std::unique_ptr<Truncate_stmt>,
  std::unique_ptr<Where_clause>,
  std::unique_ptr<Insert_stmt>,
  std::unique_ptr<Update_stmt>,
  std::unique_ptr<Delete_stmt>,
  std::unique_ptr<Create_stmt>
  >;

/**
 * @brief A top down LLA SQL Parser that builds an AST
 */
struct Parser {
  /**
   * @brief Constructs parser with a lexer
   * @param[in] lexer Lexer to use for tokens
   */
  explicit Parser(Lexer& lexer) : m_lexer(lexer) {
    m_state_stack.push_back({m_lexer.m_pos, Lexeme{}, Lexeme{}, 1, 1 });

    /* Get the first token */
    advance();
  }

  ~Parser() {
    assert(m_state_stack.size() == 1);
  }

  /**
   * @brief Parses SQL into an AST
   * 
   * @return Root node of the AST
   */
  [[nodiscard]] Ast_node parse();

private:
  void advance() {
    m_lexer.skip_whitespace();

    auto &state = m_state_stack.back();
    state.m_prev_lexeme = std::move(state.m_lexeme);
    state.m_lexeme = std::move(m_lexer.next_token().get_lexeme());
  }

  void expect(Lexeme_type type) {
    const auto &state = m_state_stack.back();
    if (state.m_lexeme.m_type != type) [[unlikely]] {
      throw std::runtime_error(
        std::format("Unexpected token type: expected token {}, got {}, previous token '{}', current token '{}, m_pos {}\n{}'",
          sql::to_string(type),
          sql::to_string(state.m_lexeme.m_type),
          state.m_prev_lexeme.m_value,
          state.m_lexeme.m_value,
          state.m_pos,
          m_lexer.m_input.substr(state.m_col)));
    }
    advance();
  }

  void expect(Lexeme_type type, const std::string& value) {
    const auto &state = m_state_stack.back();
    if (state.m_lexeme.m_type != type || state.m_lexeme.m_value != value) [[unlikely]] {
      throw std::runtime_error("Unexpected token");
    }
    advance();
  }

  [[nodiscard]] bool match(Lexeme_type type) {
    const auto &state = m_state_stack.back();
    if (state.m_lexeme.m_type == type) [[likely]] {
      advance();
      return true;
    } else {
      return false;
    }
  }

  /**
   * @brief Matches a token with a given type and value
   * 
   * If there is a match, the lexer advances to the next token
   * 
   * @param[in] type Token type
   * @param[in] value Token value
   * 
   * @return True if the token matches, false otherwise
   */
  [[nodiscard]] bool match(Lexeme_type type, const std::string& value) {
    const auto &state = m_state_stack.back();
    if (state.m_lexeme.m_type == type && state.m_lexeme.m_value == value) [[likely]] {
      advance();
      return true;
    } else {
      return false;
    }
  }

  /* Token lookahead
   *
   * @param[in] offset Offset to lookahead
   *
   * @return Lexeme at offset
   */
  [[nodiscard]] Lexeme peek(size_t offset) {
    auto &state = m_state_stack.back();

    /* Store current position */
    auto saved_pos = m_lexer.m_pos;
    auto saved_lexeme = state.m_lexeme;

    /* Advance to desired position */
    for (size_t i{}; i < offset; ++i) {
      advance();
    }

    /* Get the token at that position */
    Lexeme result = state.m_lexeme;

    /* Restore original position */
    m_lexer.m_pos = saved_pos;
    state.m_lexeme = saved_lexeme;

    return result;
  }

  [[nodiscard]] bool match_but_dont_advance(Lexeme_type type, const std::string& value) {
    const auto state = m_state_stack.back();
    return state.m_lexeme.m_type == type && state.m_lexeme.m_value == value;
  }

  [[nodiscard]] Lexeme peek() {
    return peek(1);
  }

  [[nodiscard]] std::string to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
  }

  [[nodiscard]] bool is_whitespace_before(const Lexeme& lexeme) const {
    auto &state = m_state_stack.back();
    return lexeme.m_col > state.m_col + state.m_lexeme.m_value.length();
  }

  [[nodiscard]] bool peek_is_operator(const std::string& op) {
    Lexeme next = peek();
    return next.m_type == Lexeme_type::OPERATOR && next.m_value == op;
  }

  /**
   * @brief Saves the current parser state
   * @return State identifier that can be used to restore this state
   */
  size_t save_state() {
    assert(!m_state_stack.empty());
    auto &state = m_state_stack.back();

    m_state_stack.push_back({
      state.m_pos,
      state.m_lexeme,
      state.m_prev_lexeme,
      state.m_line,
      state.m_col
    });
    return m_state_stack.size() - 1;
  }

  /**
   * @brief Restores parser state to a previously saved state
   * 
   * @param[in] state_id The state identifier returned by save_state
   */
  void restore_state(size_t state_id) {
    if (state_id >= m_state_stack.size()) [[unlikely]] {
      throw std::runtime_error("Invalid parser state ID: " + std::to_string(state_id));
    }

    /* Remove all states after this one */
    m_state_stack.resize(state_id);
  } 

  /**
   * @brief Backs up the parser by one token
   */
  void backup() {
    auto &state = m_state_stack.back();

    if (state.m_prev_lexeme.m_type == Lexeme_type::UNDEFINED) [[unlikely]] {
      throw std::runtime_error("Cannot backup: no previous token");
    }

    assert(state.m_pos <= m_lexer.m_input.size());
    assert(state.m_pos >= state.m_lexeme.m_value.length());

    /* Restore position to start of current token */
    state.m_pos -= state.m_lexeme.m_value.length();
    state.m_line = state.m_lexeme.m_line;
    state.m_col = state.m_lexeme.m_col;

    /* Restore current token to previous token */
    state.m_lexeme = std::move(state.m_prev_lexeme);

    /* Reset previous token */
    state.m_prev_lexeme = Lexeme{};
  }

  /**
   * @brief Creates a savepoint that can be used for backtracking
   */
  struct Savepoint {
    explicit Savepoint(Parser& parser) 
      : m_parser(parser), m_state_id(parser.save_state()) {}
        
    ~Savepoint() {
      if (m_active) {
        rollback();
      }
    }

    /* Commit the changes and prevent rollback */
    void commit() {
      m_active = false;
    }

    /* Explicitly rollback to the checkpoint */
    void rollback() {
      m_parser.restore_state(m_state_id);
      m_active = false;
    }

    Parser& m_parser;
    size_t m_state_id;
    bool m_active{true};
  };

  /**
   * @brief Creates a savepoint for a parsing operation
   * @return Savepoint object that will automatically rollback unless committed
   */
  Savepoint create_savepoint() {
    return Savepoint(*this);
  }

  [[nodiscard]] Ast_node parse_statement();
  [[nodiscard]] std::unique_ptr<Select_stmt> parse_select_statement();
  [[nodiscard]] std::unique_ptr<Insert_stmt> parse_insert_statement();
  [[nodiscard]] std::unique_ptr<Update_stmt> parse_update_statement();
  [[nodiscard]] std::unique_ptr<Delete_stmt> parse_delete_statement();
  [[nodiscard]] std::unique_ptr<Create_stmt> parse_create_statement();
  [[nodiscard]] std::unique_ptr<Alter_table_stmt> parse_alter_table();
  [[nodiscard]] std::unique_ptr<Truncate_stmt> parse_truncate_statement();
  [[nodiscard]] std::unique_ptr<Merge_stmt> parse_merge_statement();
  [[nodiscard]] std::unique_ptr<Grant_revoke_stmt> parse_grant_revoke_statement();
  [[nodiscard]] std::vector<std::unique_ptr<Ast_base>> parse_column_list();
  [[nodiscard]] std::unique_ptr<Drop_stmt> parse_drop_statement();  
  [[nodiscard]] std::unique_ptr<Alter_stmt> parse_alter_statement();
  [[nodiscard]] std::unique_ptr<Column_ref> parse_column_ref();
  [[nodiscard]] std::unique_ptr<Table_ref> parse_table_ref();
  [[nodiscard]] std::unique_ptr<Where_clause> parse_where_clause();
  [[nodiscard]] std::unique_ptr<Group_by> parse_group_by();
  [[nodiscard]] std::vector<std::unique_ptr<Order_by_item>> parse_order_by();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_expression();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_term();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_factor();
  [[nodiscard]] std::unique_ptr<Function_call> parse_function_call();
  [[nodiscard]] std::unique_ptr<Literal> parse_literal();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_primary();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_subquery();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_ddl_statement();
  [[nodiscard]] std::unique_ptr<Create_table_def> parse_create_table();
  [[nodiscard]] std::unique_ptr<Create_index_def> parse_create_index();
  // [[nodiscard]] std::unique_ptr<Create_view_stmt> parse_create_view();
  [[nodiscard]] std::vector<Update_stmt::Assignment> parse_update_assignments();
  [[nodiscard]] std::vector<std::vector<std::unique_ptr<Ast_base>>> parse_value_lists();
  [[nodiscard]] std::optional<size_t> parse_limit();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_column_expression();
  [[nodiscard]] std::unique_ptr<Table_constraint> parse_table_constraint();
  [[nodiscard]] std::vector<std::string> parse_column_list_in_parentheses();
  [[nodiscard]] std::vector<Column_reference> parse_column_list_with_options();
  [[nodiscard]] std::unique_ptr<Table_ref> parse_table_reference();
  [[nodiscard]] std::vector<std::unique_ptr<Table_ref>> parse_table_references();
  [[nodiscard]] std::unique_ptr<Ast_base> parse_case_expression();
  [[nodiscard]] std::unique_ptr<Column_def> parse_column_definition();
  [[nodiscard]] std::unique_ptr<Create_view_def> parse_create_view();
  [[nodiscard]] Data_type parse_data_type();    
  [[nodiscard]] std::unique_ptr<Function_call> parse_optional_over_clause(std::unique_ptr<Function_call> func); 
  [[nodiscard]] std::unique_ptr<Window_spec> parse_window_specification();
  [[nodiscard]] Window_spec::Partition parse_partition_by();
  [[nodiscard]] std::unique_ptr<Window_spec::Frame> parse_frame_clause(Window_spec::Frame::Type frame_type);  
  [[nodiscard]] Window_spec::Frame::Bound parse_frame_bound();
  [[nodiscard]] Foreign_key_reference::Action parse_reference_option();
  [[nodiscard]] std::unique_ptr<Foreign_key_reference> parse_foreign_key_reference();
  [[nodiscard]] std::optional<Binary_op::Op_type> parse_operator();

  void parse_table_options(Create_table_def* table);

private:
  struct State {
    size_t m_pos{};
    Lexeme m_lexeme;
    Lexeme m_prev_lexeme;
    size_t m_line{1};
    size_t m_col{1};
  };

  Lexer& m_lexer;
  std::string_view m_input;

  /* Stack for backtracking */
  std::vector<State> m_state_stack;
};

} // namespace sql

