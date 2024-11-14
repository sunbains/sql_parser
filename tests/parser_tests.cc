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

#include "sql/parser.h"

namespace sql::test {

class Parser_test : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(Parser_test, ParsesSimpleSelect) {
  sql::Lexer lexer("SELECT id, name FROM users WHERE age >= 18");
  sql::Parser parser(lexer);
  
  auto ast = parser.parse();

  ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Select_stmt>>(ast));
  auto select = std::move(std::get<std::unique_ptr<Select_stmt>>(ast));
  ASSERT_NE(select, nullptr);
  
  EXPECT_EQ(select->m_columns.size(), 2);
  EXPECT_FALSE(select->m_distinct);
  ASSERT_EQ(select->m_from.size(), 1);
  ASSERT_TRUE(select->m_where->has_value());
}

TEST_F(Parser_test, ParsesJoins) {
  sql::Lexer lexer(
    "SELECT u.id, o.order_id "
    "FROM users u "
    "INNER JOIN orders o ON u.id = o.user_id"
  );
  sql::Parser parser(lexer);
  
  auto ast = parser.parse();

  ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Select_stmt>>(ast));
  auto select = std::move(std::get<std::unique_ptr<Select_stmt>>(ast));
  ASSERT_NE(select, nullptr);
  
  const auto n_from = select->m_from.size();
  ASSERT_EQ(n_from, 1);

  const auto join_ref = dynamic_cast<Join_ref*>(select->m_from[0].get());
  ASSERT_NE(join_ref, nullptr);

  EXPECT_EQ(join_ref->m_join->m_type, Join_type::inner);
}

} // namespace sql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

