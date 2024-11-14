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

#include <coroutine>
#include <exception>
#include <format>
#include <cctype>
#include <memory>
#include <string>
#include <string_view>

namespace sql {

/**
 * @brief Token types for SQL lexical analysis
 */
enum class Lexeme_type {
  UNDEFINED,
  KEYWORD,
  IDENTIFIER,
  NUMBER,
  STRING_LITERAL,
  OPERATOR,
  PUNCTUATION,
  WHITESPACE,
  END_OF_FILE
};

inline const char *to_string(Lexeme_type type) noexcept {
  switch (type) {
    case Lexeme_type::UNDEFINED:
      return "UNDEFINED";
    case Lexeme_type::KEYWORD:
      return "KEYWORD";
    case Lexeme_type::IDENTIFIER:
      return "IDENTIFIER";
    case Lexeme_type::NUMBER:
      return "NUMBER";
    case Lexeme_type::STRING_LITERAL:
      return "STRING_LITERAL";
    case Lexeme_type::OPERATOR:
      return "OPERATOR";
    case Lexeme_type::PUNCTUATION:
      return "PUNCTUATION";
    case Lexeme_type::WHITESPACE:
      return "WHITESPACE";
    case Lexeme_type::END_OF_FILE:
      return "END_OF_FILE";
  }
  std::terminate();
} 

struct Lexeme {
  std::string to_string() const noexcept {
    return std::format(
      "{{ m_type: {}, m_value: {}, m_line: {}, m_col: {} }}",
      sql::to_string(m_type), m_value, m_line, m_col);
  }

  Lexeme_type m_type{Lexeme_type::UNDEFINED};
  std::string m_value{};
  size_t m_line{};
  size_t m_col{};
};

/**
 * @brief Represents a single token in SQL
 */
struct Token {
  struct promise_type;
  using handle_type = std::coroutine_handle<promise_type>;

  Token(handle_type handle) noexcept
    : m_handle(handle) {}

  ~Token() noexcept {
    if (m_handle) {
      m_handle.destroy();
    }
  }

  Token(Token&& rhs) noexcept 
    : m_handle(rhs.m_handle) {
    rhs.m_handle = nullptr;
  }

  Token& operator=(Token&& rhs) noexcept {
    if (this != &rhs) {
      if (m_handle) {
        m_handle.destroy();
        m_handle = rhs.m_handle;
        rhs.m_handle = nullptr;
      }
    }
    return *this;
  }

  void resume() {
    if (m_handle && !is_done()) {
      m_handle.resume();
    }
  }

  bool is_done() const noexcept {
    return m_handle.done();
  }

  promise_type& promise() const noexcept {
   return m_handle.promise();
  }

  inline Lexeme get_lexeme();

  handle_type m_handle{};
};

/**
  * @brief Coroutine promise type for token generation
  */
struct Token::promise_type {
  auto get_return_object() {
    return std::coroutine_handle<promise_type>::from_promise(*this);
  }
      
  auto initial_suspend() {
    return std::suspend_never{};
  }

  auto final_suspend() noexcept {
    return std::suspend_always{};
  }

  void unhandled_exception() {
    m_exception = std::current_exception();
  }
      
  inline auto yield_value(Token token);

  void return_value(Lexeme lexeme) {
    m_lexeme = std::move(lexeme);
  }

  Lexeme m_lexeme;
  std::exception_ptr m_exception;
};

inline Lexeme Token::get_lexeme() {
  if (m_handle.promise().m_exception) {
    std::rethrow_exception(m_handle.promise().m_exception);
  } else {
    return std::move(m_handle.promise().m_lexeme);
  }
}

inline auto Token::promise_type::yield_value(Token token) {
  m_lexeme = std::move(token.get_lexeme());
  return std::suspend_always{};
} 

/**
 * @brief SQL Lexer that uses coroutines for token generation
 */
struct Lexer {
  constexpr static auto Eof = Lexeme{Lexeme_type::END_OF_FILE, "", 0, 0};

  /**
   * @brief Constructs a lexer with input text
   *
   * @param[in] input SQL text to tokenize
   */
  explicit Lexer(std::string_view input);

  inline void skip_whitespace() noexcept {
    while (m_pos < m_input.size() && std::isspace(m_input[m_pos])) {
      if (m_input[m_pos] == '\n') [[unlikely]] {
        ++m_line;
        m_col = 1;
      } else {
        ++m_col;
      }
      ++m_pos;
    }
  }

  [[nodiscard]] inline char peek(size_t offset) const noexcept {
    if (m_pos + offset >= m_input.size()) [[unlikely]] {
      return '\0';
    } else {
      return m_input[m_pos + offset];
    }
  }

  [[nodiscard]] inline char peek() const noexcept {
    return peek(0);
  }

  [[nodiscard]] char advance() noexcept {
    if (m_pos >= m_input.size()) [[unlikely]] {
        return '\0';
    } else {
      const char c = m_input[m_pos++];
      ++m_col;
      return c;
    }
  }

  /**
   * @brief Generates next token
   * @return Token coroutine that yields next token
   */
  [[nodiscard]] Token next_token() {
    while (m_pos < m_input.size()) {
      skip_whitespace();

      if (m_pos >= m_input.size()) [[unlikely]] {
        break;
      }

      char c = peek();

      if (std::isalpha(c) || c == '_') {
        co_yield lex_identifier();
      } else if (std::isdigit(c)) {
        co_yield lex_number();
      } else if (c == '\'') {
        co_yield lex_string();
      } else {
        co_yield lex_operator();
      }
    }
    co_return Eof;
  }

private:
  [[nodiscard]] Token lex_number();
  [[nodiscard]] Token lex_string();
  [[nodiscard]] Token lex_operator();
  [[nodiscard]] Token lex_identifier();

private:
  size_t m_pos{};
  size_t m_line{};
  size_t m_col{};
  std::string_view m_input{};

  friend struct Parser;
};

} // namespace sql

