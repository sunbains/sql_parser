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
  Alter_table_stmt,
  Column_ref,
  Create_table_stmt,
  Create_trigger_stmt,
  Create_view_stmt,
  Drop_stmt,
  Grant_revoke_stmt,
  Merge_stmt,
  Select_stmt,
  Truncate_stmt,
  Where_clause,
  Insert_stmt,
  Update_stmt,
  Delete_stmt,
  Create_stmt
  >;

/**
 * @brief SQL Parser that builds an AST
 */
struct Parser {
  /**
   * @brief Constructs parser with a lexer
   * @param[in] lexer Lexer to use for tokens
   */
  explicit Parser(Lexer& lexer) : m_lexer(lexer) {}

  /**
   * @brief Parses SQL into AST
   * 
   * @return Root node of AST
   */
  [[nodiscard]] std::unique_ptr<Ast_node> parse();

private:
  void advance() {
    m_lexeme = m_lexer.next_token().get_lexeme();
  }

  void expect(Lexeme_type type) {
    if (m_lexeme.m_type != type) [[unlikely]] {
      throw std::runtime_error("Unexpected token type");
    }
    advance();
  }

  void expect(Lexeme_type type, const std::string& value) {
    if (m_lexeme.m_type != type || m_lexeme.m_value != value) [[unlikely]] {
      throw std::runtime_error("Unexpected token");
    }
    advance();
  }

  [[nodiscard]] bool match(Lexeme_type type) {
    if (m_lexeme.m_type == type) [[likely]] {
      advance();
      return true;
    } else {
      return false;
    }
  }

  [[nodiscard]] bool match(Lexeme_type type, const std::string& value) {
    if (m_lexeme.m_type == type && m_lexeme.m_value == value) [[likely]] {
      advance();
      return true;
    } else {
      return false;
    }
  }

  struct Column_reference {
    std::optional<std::string> m_schema;
    std::optional<std::string> m_table;
    std::string m_column;
    bool m_ascending{true};
    enum class Nulls_position {
        default_,
        first,
        last
    } m_nulls_position{Nulls_position::default_};
    /* For index prefix lengths   */
    std::optional<size_t> m_length;  
    /* For collations */
    std::optional<std::string> m_collation;
  };

  /* Token lookahead
   *
   * @param[in] offset Offset to lookahead
   *
   * @return Lexeme at offset
   */
  [[nodiscard]] Lexeme peek(size_t offset) {
    /* Store current position */
    auto saved_pos = m_lexer.m_pos;
    auto saved_token = m_lexeme;

    /* Advance to desired position */
    for (size_t i{}; i < offset; ++i) {
      advance();
    }

    /* Get the token at that position */
    Lexeme result = m_lexeme;

    /* Restore original position */
    m_lexer.m_pos = saved_pos;
    m_lexeme = saved_token;

    return result;
  }

  [[nodiscard]] Lexeme peek() {
    return peek(1);
  }

  [[nodiscard]] std::string to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
  }

  [[nodiscard]] bool is_whitespace_before(const Lexeme& lexeme) const {
    return lexeme.m_column > m_previous_lexeme.m_column + m_previous_lexeme.m_value.length();
  }

  [[nodiscard]] bool peek_is_operator(const std::string& op) {
    Lexeme next = peek();
    return next.m_type == Lexeme_type::operator_ && next.m_value == op;
  }

  /**
   * @brief Saves the current parser state
   * @return State identifier that can be used to restore this state
   */
  size_t save_state() {
    m_state_stack.push_back({
      m_position,
      m_current_token,
      m_previous_token,
      m_line,
      m_column
    });
    return m_state_stack.size() - 1;
  }

  /**
   * @brief Restores parser state to a previously saved state
   * @param state_id The state identifier returned by save_state
   */
  void restore_state(size_t state_id) {
    if (state_id >= m_state_stack.size()) [[unlikely]] {
      throw std::runtime_error("Invalid parser state ID");
    }

    const auto& state = m_state_stack[state_id];

    m_position = state.m_position;
    m_current_token = state.m_current_token;
    m_previous_token = state.m_previous_token;
    m_line = state.m_line;
    m_column = state.m_column;
        
    /* Remove all states after this one */
    m_state_stack.resize(state_id);
  } 

  /**
   * @brief Backs up the parser by one token
   */
  void backup() {
    if (m_previous_token.m_type == Lexeme_type::undefined) [[unlikely]] {
      throw std::runtime_error("Cannot backup: no previous token");
    }

    assert(m_position >= m_current_token.m_value.length());

    /* Restore position to start of current token */
    m_position -= m_current_token.m_value.length();
    m_line = m_current_token.m_line;
    m_column = m_current_token.m_column;

    /* Restore current token to previous token */
    m_current_token = m_previous_token;

    /* Reset previous token */
    m_previous_token = Lexeme{};
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

  [[nodiscard]] std::unique_ptr<Ast_base> parse_statement();
  [[nodiscard]] std::unique_ptr<Select_stmt> parse_select_statement();
  [[nodiscard]] std::unique_ptr<Insert_stmt> parse_insert_statement();
  [[nodiscard]] std::unique_ptr<Update_stmt> parse_update_statement();
  [[nodiscard]] std::unique_ptr<Delete_stmt> parse_delete_statement();
  [[nodiscard]] std::unique_ptr<Create_stmt> parse_create_statement();
  [[nodiscard]] std::unique_ptr<Truncate_stmt> parse_truncate_statement();
  [[nodiscard]] std::unique_ptr<Merge_stmt> parse_merge_statement();
  [[nodiscard]] std::unique_ptr<Grant_revoke_stmt> parse_grant_revoke_statement();
  [[nodiscard]] std::vector<std::unique_ptr<Ast_base>> parse_column_list();
  [[nodiscard]] std::unique_ptr<Drop_stmt> parse_drop_statement();  
  [[nodiscard]] std::unique_ptr<Alter_table_stmt> parse_alter_statement();
  [[nodiscard]] std::unique_ptr<Column_ref> parse_column_ref();
  [[nodiscard]] std::unique_ptr<Table_ref> parse_table_ref();
  [[nodiscard]] std::unique_ptr<Where_clause> parse_where_clause();
  [[nodiscard]] std::unique_ptr<Group_by> parse_group_by();
  [[nodiscard]] std::vector<std::unique_ptr<Order_by_item>> parse_order_by();
  [[nodiscard]] std::unique_ptr<Binary_op> parse_expression();
  [[nodiscard]] std::unique_ptr<Binary_op> parse_term();
  [[nodiscard]] std::unique_ptr<Binary_op> parse_factor();
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
  void parse_table_options(Create_table_def* table);

private:
  struct State {
    size_t m_position;
    Lexeme m_current_token;
    Lexeme m_previous_token;
    size_t m_line;
    size_t m_column;
  };

  /* State management */
  std::string_view m_input;
  size_t m_position{0};
  Lexeme m_current_token;
  Lexeme m_previous_token;
  size_t m_line{1};
  size_t m_column{1};
    
  /* Stack for backtracking */
  std::vector<State> m_state_stack;

  Lexer& m_lexer;
  Lexeme m_lexeme;
  Lexeme m_previous_lexeme;
};

} // namespace sql

