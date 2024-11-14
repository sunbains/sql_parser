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

#include <algorithm>
#include <cassert>
#include <iostream>
#include <unordered_set>

#include "sql/lexer.h"

namespace sql {

Lexer::Lexer(std::string_view input) 
  : m_pos(), m_line(1), m_col(1), m_input(input) {}

Token Lexer::lex_identifier() {
  const auto start_pos{m_pos};
  const auto start_col{m_col};

  while (m_pos < m_input.size() && (std::isalnum(m_input[m_pos]) || m_input[m_pos] == '_')) {
    ++m_pos;
    ++m_col;
  }

  const std::string value(m_input.substr(start_pos, m_pos - start_pos));

  // TODO: Add more keywords
  static const std::unordered_set<std::string> keywords = {
    "AND", "ASC", "BY", "CROSS", "DELETE", "DESC", "DISTINCT", "FALSE", "FETCH", "FIRST", "FROM", "FULL",
    "GROUP", "HAVING", "INNER", "INSERT", "JOIN", "LAST", "LEFT", "LIKE", "LIMIT", "NATURAL", "NEXT", "NOT",
    "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "RECURSIVE", "RIGHT", "ROW", "ROWS", "SELECT",
    "SET", "TRUE", "UPDATE", "USING", "WHERE", "WITH", "WITHOUT"
  };

  std::string ucase{value};
  std::transform(ucase.begin(), ucase.end(), ucase.begin(), ::toupper);
  Lexeme_type type = keywords.contains(ucase) ? Lexeme_type::KEYWORD : Lexeme_type::IDENTIFIER;

  co_return Lexeme{type, value, m_line, start_col};
}

Token Lexer::lex_number() {
  bool has_decimal{};
  const auto start_pos{m_pos};
  const auto start_col{m_col};

  while (m_pos < m_input.size()) {
    if (m_input[m_pos] == '.' && !has_decimal) {
      has_decimal = true;
      ++m_pos;
      ++m_col;
    } else if (std::isdigit(m_input[m_pos])) {
      ++m_pos;
      ++m_col;
    } else {
      break;
    }
  }

  co_return Lexeme{Lexeme_type::NUMBER, std::string(m_input.substr(start_pos, m_pos - start_pos)), m_line, start_col};
}

Token Lexer::lex_string() {
  assert(m_input[m_pos] == '\'');

  /* Skip opening quote */
  ++m_pos;

  std::string value{};
  const auto start_col{m_col};

  value.reserve(32);

  while (m_pos < m_input.size()) {
    if (m_input[m_pos] == '\\' && m_pos + 1 < m_input.size()) {
      /* Handle escape sequence */
      value.push_back(m_input[m_pos + 1]);
      m_pos += 2;
      m_col += 2;
    } else if (m_input[m_pos] == '\'' && m_pos + 1 < m_input.size() && m_input[m_pos + 1] == '\'') {
      /* Handle SQL single quote escape */
      value.push_back('\'');
      m_pos += 2;
      m_col += 2;
    } else if (m_input[m_pos] == '\'') {
      break;
    } else {
      value.push_back(m_input[m_pos]);
      ++m_pos;
      ++m_col;
    }
  }

  if (m_pos >= m_input.size()) {
    throw std::runtime_error("Unterminated string literal");
  }

  assert(m_input[m_pos] == '\'');

  /* Skip closing quote */
  ++m_pos;
  ++m_col;

  co_return Lexeme{Lexeme_type::STRING_LITERAL, value, m_line, start_col};
}

Token Lexer::lex_operator() {
  size_t start_col = m_col;
  char c = m_input[m_pos++];

  ++m_col;

  /* Handle multi-character operators */
  if (m_pos < m_input.size()) {
    std::string op{c, m_input[m_pos]};
    if (op == "<=" || op == ">=" || op == "!=" || op == "<>") {
      ++m_pos;
      ++m_col;
      co_return Lexeme{Lexeme_type::OPERATOR, op, m_line, start_col};
    }
  }

  co_return Lexeme{Lexeme_type::OPERATOR, std::string(1, c), m_line, start_col};
}

} // namespace sql
