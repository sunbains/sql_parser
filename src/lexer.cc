#include <unordered_set>

#include "sql/lexer.h"

namespace sql {

Lexer::Lexer(std::string_view input) 
  : m_pos(), m_line(1), m_column(1), m_input(input) {}

Token Lexer::lex_identifier() {
  size_t start_pos = m_pos;
  size_t start_column = m_column;

  while (m_pos < m_input.size() && (std::isalnum(m_input[m_pos]) || m_input[m_pos] == '_')) {
    ++m_pos;
    ++m_column;
  }

  std::string value(m_input.substr(start_pos, m_pos - start_pos));

  static const std::unordered_set<std::string> keywords = {
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT",
    "INSERT", "UPDATE", "DELETE", "AND", "OR", "NOT", "NULL", "TRUE",
    "FALSE"
  };

  Lexeme_type type = keywords.contains(value) ? Lexeme_type::keyword : Lexeme_type::identifier;

  co_return Lexeme{type, value, m_line, start_column};
}

Token Lexer::lex_number() {
  size_t start_pos = m_pos;
  size_t start_column = m_column;
  bool has_decimal = false;

  while (m_pos < m_input.size()) {
    if (m_input[m_pos] == '.' && !has_decimal) {
      has_decimal = true;
      ++m_pos;
      ++m_column;
    } else if (std::isdigit(m_input[m_pos])) {
      ++m_pos;
      ++m_column;
    } else {
      break;
    }
  }

  co_return Lexeme{Lexeme_type::number, std::string(m_input.substr(start_pos, m_pos - start_pos)), m_line, start_column};
}

Token Lexer::lex_string() {
  std::string value;

  /* Skip opening quote */
  ++m_pos;

  size_t start_column = m_column++;

  while (m_pos < m_input.size() && m_input[m_pos] != '\'') {
    if (m_input[m_pos] == '\\' && m_pos + 1 < m_input.size()) {
      value += m_input[m_pos + 1];
      m_pos += 2;
      m_column += 2;
    } else {
      value += m_input[m_pos];
      ++m_pos;
      ++m_column;
    }
  }

  if (m_pos >= m_input.size()) {
    throw std::runtime_error("Unterminated string literal");
  }

  /* Skip closing quote */
  ++m_pos;
  ++m_column;

  co_return Lexeme{Lexeme_type::string_literal, value, m_line, start_column};
}

Token Lexer::lex_operator() {
  size_t start_column = m_column;
  char c = m_input[m_pos++];

  ++m_column;

  /* Handle multi-character operators */
  if (m_pos < m_input.size()) {
    std::string op{c, m_input[m_pos]};
    if (op == "<=" || op == ">=" || op == "!=" || op == "<>") {
      ++m_pos;
      ++m_column;
      co_return Lexeme{Lexeme_type::operator_, op, m_line, start_column};
    }
  }

  co_return Lexeme{Lexeme_type::operator_, std::string(1, c), m_line, start_column };
}

} // namespace sql
