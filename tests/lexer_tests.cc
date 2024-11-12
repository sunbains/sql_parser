#include <gtest/gtest.h>
#include <memory>

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
};

TEST_F(Lexer_test, TokenizesSimpleSelect) {
  sql::Lexer lexer("SELECT id FROM users");
  auto token = lexer.next_token();
  auto& promise = token.promise();
  
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::KEYWORD);
  EXPECT_EQ(promise.m_lexeme.m_value, "SELECT");
  
  token.resume();
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::IDENTIFIER);
  EXPECT_EQ(promise.m_lexeme.m_value, "id");
  
  token.resume();
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::KEYWORD);
  EXPECT_EQ(promise.m_lexeme.m_value, "FROM");
  
  token.resume();
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::IDENTIFIER);
  EXPECT_EQ(promise.m_lexeme.m_value, "users");
}

TEST_F(Lexer_test, HandlesWhitespace) {
  sql::Lexer lexer("SELECT    id   FROM\n\tusers");
  auto token = lexer.next_token();
  auto& promise = token.promise();
  
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::KEYWORD);
  EXPECT_EQ(promise.m_lexeme.m_value, "SELECT");
  
  token.resume();
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::IDENTIFIER);
  EXPECT_EQ(promise.m_lexeme.m_value, "id");
}

TEST_F(Lexer_test, TokenizesStrings) {
  sql::Lexer lexer("SELECT 'hello world' AS greeting");
  auto token= lexer.next_token();
  auto& promise = token.promise();
  
  /* Skip SELECT */
  token.resume();
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(promise.m_lexeme.m_value, "hello world");
}

TEST_F(Lexer_test, TokenizesNumbers_1) {
  sql::Lexer lexer("WHERE age > 25.5");
  auto token = lexer.next_token();
  auto& promise = token.promise();
  
  /* Skip WHERE */
  token.resume();
  /* Skip age */
  token.resume();
  /* Skip > */
  token.resume();
  
  EXPECT_EQ(promise.m_lexeme.m_type, sql::Lexeme_type::NUMBER);
  EXPECT_EQ(promise.m_lexeme.m_value, "25.5");
}

// Basic token type tests
TEST_F(Lexer_test, TokenizesKeywords) {
  auto tokens = get_all_tokens("SELECT FROM WHERE GROUP BY HAVING ORDER");

  ASSERT_EQ(tokens.size(), 7);
  for (const auto& token : tokens) {
    EXPECT_EQ(token.m_type, Lexeme_type::KEYWORD);
  }

  EXPECT_EQ(tokens[0].m_value, "SELECT");
  EXPECT_EQ(tokens[1].m_value, "FROM");
  EXPECT_EQ(tokens[2].m_value, "WHERE");
  EXPECT_EQ(tokens[3].m_value, "GROUP");
  EXPECT_EQ(tokens[4].m_value, "BY");
  EXPECT_EQ(tokens[5].m_value, "HAVING");
  EXPECT_EQ(tokens[6].m_value, "ORDER");
}

TEST_F(Lexer_test, TokenizesIdentifiers) {
  auto tokens = get_all_tokens("table_name column1 my_identifier123");

  ASSERT_EQ(tokens.size(), 3);
  for (const auto& token : tokens) {
    EXPECT_EQ(token.m_type, Lexeme_type::IDENTIFIER);
  }

  EXPECT_EQ(tokens[0].m_value, "table_name");
  EXPECT_EQ(tokens[1].m_value, "column1");
  EXPECT_EQ(tokens[2].m_value, "my_identifier123");
}

TEST_F(Lexer_test, TokenizesNumbers_2) {
  auto tokens = get_all_tokens("42 3.14 0.123 42.0");

  ASSERT_EQ(tokens.size(), 4);
  for (const auto& token : tokens) {
    EXPECT_EQ(token.m_type, Lexeme_type::NUMBER);
  }

  EXPECT_EQ(tokens[0].m_value, "42");
  EXPECT_EQ(tokens[1].m_value, "3.14");
  EXPECT_EQ(tokens[2].m_value, "0.123");
  EXPECT_EQ(tokens[3].m_value, "42.0");
}

TEST_F(Lexer_test, TokenizesStringLiterals) {
  auto tokens = get_all_tokens("'hello' 'with space' 'with''quote' 'with\\'escape'");

  ASSERT_EQ(tokens.size(), 4);
  for (const auto& token : tokens) {
    EXPECT_EQ(token.m_type, Lexeme_type::STRING_LITERAL);
  }

  EXPECT_EQ(tokens[0].m_value, "hello");
  EXPECT_EQ(tokens[1].m_value, "with space");
  EXPECT_EQ(tokens[2].m_value, "with'quote");
  EXPECT_EQ(tokens[3].m_value, "with'escape");
}

TEST_F(Lexer_test, TokenizesOperators) {
  auto tokens = get_all_tokens("= <> < > <= >= + - * / %");

  ASSERT_EQ(tokens.size(), 11);
  for (const auto& token : tokens) {
    EXPECT_EQ(token.m_type, Lexeme_type::OPERATOR);
  }

  EXPECT_EQ(tokens[0].m_value, "=");
  EXPECT_EQ(tokens[1].m_value, "<>");
  EXPECT_EQ(tokens[2].m_value, "<");
  EXPECT_EQ(tokens[3].m_value, ">");
  EXPECT_EQ(tokens[4].m_value, "<=");
  EXPECT_EQ(tokens[5].m_value, ">=");
  EXPECT_EQ(tokens[6].m_value, "+");
  EXPECT_EQ(tokens[7].m_value, "-");
  EXPECT_EQ(tokens[8].m_value, "*");
  EXPECT_EQ(tokens[9].m_value, "/");
  EXPECT_EQ(tokens[10].m_value, "%");
}

// Complex statement tests
TEST_F(Lexer_test, TokenizesCompleteSelectStatement) {
  auto tokens = get_all_tokens(
    "SELECT id, name, age "
    "FROM users "
    "WHERE age >= 18 "
    "AND name LIKE 'John%' "
    "ORDER BY name DESC "
    "LIMIT 10"
  );

  ASSERT_GT(tokens.size(), 0);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::KEYWORD);
  EXPECT_EQ(tokens[0].m_value, "SELECT");
}

TEST_F(Lexer_test, TokenizesCompleteInsertStatement) {
  auto tokens = get_all_tokens(
    "INSERT INTO users (name, age, email) "
    "VALUES ('John Doe', 25, 'john@example.com')"
  );

  ASSERT_GT(tokens.size(), 0);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::KEYWORD);
  EXPECT_EQ(tokens[0].m_value, "INSERT");
}

// Whitespace handling tests
TEST_F(Lexer_test, HandlesVariousWhitespace) {
  auto tokens = get_all_tokens("SELECT\n\tid,\r\nname,  age\t\tFROM users");

  ASSERT_EQ(tokens.size(), 8);
  ASSERT_EQ(tokens[0].m_type, Lexeme_type::KEYWORD);
  EXPECT_EQ(tokens[0].m_value, "SELECT");
  ASSERT_EQ(tokens[1].m_type, Lexeme_type::IDENTIFIER);
  EXPECT_EQ(tokens[1].m_value, "id");
  ASSERT_EQ(tokens[2].m_type, Lexeme_type::OPERATOR);
  EXPECT_EQ(tokens[2].m_value, ",");
  ASSERT_EQ(tokens[3].m_type, Lexeme_type::IDENTIFIER);
  EXPECT_EQ(tokens[3].m_value, "name");
  ASSERT_EQ(tokens[4].m_type, Lexeme_type::OPERATOR);
  EXPECT_EQ(tokens[4].m_value, ",");
  ASSERT_EQ(tokens[5].m_type, Lexeme_type::IDENTIFIER);
  EXPECT_EQ(tokens[5].m_value, "age");
  ASSERT_EQ(tokens[6].m_type, Lexeme_type::KEYWORD);
  EXPECT_EQ(tokens[6].m_value, "FROM");
  ASSERT_EQ(tokens[7].m_type, Lexeme_type::IDENTIFIER);
  EXPECT_EQ(tokens[7].m_value, "users");
}

// Error handling tests
TEST_F(Lexer_test, ThrowsOnUnterminatedString) {
  EXPECT_THROW(get_all_tokens("SELECT 'unterminated"), std::runtime_error);
}

TEST_F(Lexer_test, HandlesEmptyInput) {
  auto tokens = get_all_tokens("");
  EXPECT_EQ(tokens.size(), 0);
}

// Line and column tracking tests
TEST_F(Lexer_test, TracksLineAndColumnNumbers) {
  auto tokens = get_all_tokens(
    "SELECT id,\n"
    "       name,\n"
    "       age\n"
    "FROM users"
  );

  ASSERT_GT(tokens.size(), 0);
  EXPECT_EQ(tokens[0].m_line, 1);
  EXPECT_EQ(tokens[0].m_col, 1);

  // Find 'FROM' token which should be on line 4
  auto from_token = std::find_if(tokens.begin(), tokens.end(),
    [](const Lexeme& t) { return t.m_value == "FROM"; });
  ASSERT_NE(from_token, tokens.end());
  EXPECT_EQ(from_token->m_line, 4);
  EXPECT_EQ(from_token->m_col, 1);
}

// Case sensitivity tests
TEST_F(Lexer_test, HandlesKeywordCasing) {
  auto tokens1 = get_all_tokens("SELECT FROM WHERE");
  auto tokens2 = get_all_tokens("select from where");
  auto tokens3 = get_all_tokens("Select From Where");

  ASSERT_EQ(tokens1.size(), tokens2.size());
  ASSERT_EQ(tokens2.size(), tokens3.size());

  for (size_t i = 0; i < tokens1.size(); ++i) {
    EXPECT_EQ(tokens1[i].m_type, Lexeme_type::KEYWORD);
    EXPECT_EQ(tokens2[i].m_type, Lexeme_type::KEYWORD);
    EXPECT_EQ(tokens3[i].m_type, Lexeme_type::KEYWORD);
  }
}

// Special character handling
TEST_F(Lexer_test, HandlesSpecialCharactersInIdentifiers) {
  auto tokens = get_all_tokens("table_name column_2 _prefix");

  ASSERT_EQ(tokens.size(), 3);
  for (const auto& token : tokens) {
    EXPECT_EQ(token.m_type, Lexeme_type::IDENTIFIER);
  }
}

TEST_F(Lexer_test, HandlesSpecialCharactersInStrings) {
  auto tokens = get_all_tokens("'Special @#$%^&* characters'");

  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0].m_type, Lexeme_type::STRING_LITERAL);
  EXPECT_EQ(tokens[0].m_value, "Special @#$%^&* characters");
}

} // namespace sql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

