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

#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>

#include "sql/lexer.h"

namespace sql::test {

class Lexer_test : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}

  // Helper function to get next token
  Lexeme get_next_token(Lexer& lexer) {
    auto token = lexer.next_token();
    return token.get_lexeme();
  }

  // Helper to get all tokens
  std::vector<Lexeme> get_all_tokens(const std::string& input) {
    Lexer lexer(input);
    std::vector<Lexeme> tokens;
    
    while (true) {
      auto token = get_next_token(lexer);
      if (token.m_type == Lexeme_type::END_OF_FILE) {
        break;
      }
      tokens.push_back(token);
    }
    
    return tokens;
  }

  // Helper to assert that lexing throws an exception
  void expect_lexer_error(const std::string& input, const std::string& expected_error = "") {
    try {
      get_all_tokens(input);
      FAIL() << "Expected lexer error for input: " << input;
    } catch (const std::runtime_error& e) {
      if (!expected_error.empty()) {
        EXPECT_EQ(e.what(), expected_error);
      }
    }
  }
};

// Previous tests remain the same...

// Malformed string literal tests
TEST_F(Lexer_test, DetectsUnterminatedStringLiterals) {
  // Basic unterminated string
  expect_lexer_error("'unterminated");
  
  // Unterminated string with escaped quote
  expect_lexer_error("'string with escaped quote\\'");
  
  // Unterminated string after valid string
  expect_lexer_error("'valid' 'unterminated");
  
  // Unterminated string in SQL statement
  expect_lexer_error("SELECT * FROM users WHERE name = 'incomplete");
}

TEST_F(Lexer_test, DetectsInvalidEscapeSequences) {
  // Invalid escape at end of string
  expect_lexer_error("'invalid escape\\");
  
  // Invalid escape followed by quote
  expect_lexer_error("'invalid escape\\'");
}

TEST_F(Lexer_test, HandlesMultipleQuotesCorrectly) {
  // Test empty string
  auto tokens = get_all_tokens("''");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "");

  // Test doubled quotes (SQL escape syntax for single quote)
  tokens = get_all_tokens("'It''s working'");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "It's working");

  // Test multiple doubled quotes
  tokens = get_all_tokens("'multiple''quotes''here'");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "multiple'quotes'here");
}

TEST_F(Lexer_test, HandlesMixedQuoteAndEscapeSequences) {
  // Test mix of doubled quotes and escape sequences
  auto tokens = get_all_tokens("'mix of ''quote'' and \\'escape\\''");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "mix of 'quote' and 'escape'");
}

TEST_F(Lexer_test, HandlesStringLiteralsWithSpecialCharacters) {
  // Test newlines in strings
  auto tokens = get_all_tokens("'contains\nnewline'");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "contains\nnewline");
  
  // Test tabs in strings
  tokens = get_all_tokens("'contains\ttab'");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "contains\ttab");
  
  // Test backslash followed by normal characters
  tokens = get_all_tokens("'back\\slash'");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "backslash");
}

TEST_F(Lexer_test, HandlesEmptyAndWhitespaceStrings) {
  // Empty string
  auto tokens = get_all_tokens("''");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "");

  // String with only spaces
  tokens = get_all_tokens("'   '");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "   ");

  // String with mixed whitespace
  tokens = get_all_tokens("'  \t  '");
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "  \t  ");
}

TEST_F(Lexer_test, HandlesConsecutiveStringLiterals) {
  // Multiple string literals separated by whitespace
  auto tokens = get_all_tokens("'first' 'second' 'third'");
  ASSERT_EQ(tokens.size(), 3);
  EXPECT_EQ(tokens[0].m_value, "first");
  EXPECT_EQ(tokens[1].m_value, "second");
  EXPECT_EQ(tokens[2].m_value, "third");
}

TEST_F(Lexer_test, HandlesStringLiteralsInSQLStatements) {
  // Test string literals in typical SQL contexts
  auto tokens = get_all_tokens(
    "SELECT * FROM users WHERE name = 'John''s' AND city = 'New York'"
  );
  
  // Find the string literals and verify them
  auto it1 = std::find_if(tokens.begin(), tokens.end(),
    [](const Lexeme& t) { return t.m_value == "John's"; });
  auto it2 = std::find_if(tokens.begin(), tokens.end(),
    [](const Lexeme& t) { return t.m_value == "New York"; });
    
  ASSERT_NE(it1, tokens.end());
  ASSERT_NE(it2, tokens.end());
  EXPECT_EQ(it1->m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(it2->m_type, Lexeme_type::STRING_LITERAL);
}

// Previous main() function remains the same...

} // namespace sql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

