#include <gtest/gtest.h>
#include <memory>
#include <variant>

#include "sql/parser.h"

namespace sql::test {

class Select_test : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}

  template<typename T>
  std::unique_ptr<T> parse_sql(const std::string& sql) {
    Lexer lexer(sql);
    Parser parser(lexer);
    auto ast = parser.parse();
    
    EXPECT_TRUE(std::holds_alternative<std::unique_ptr<T>>(ast));
    if (!std::holds_alternative<std::unique_ptr<T>>(ast)) {
      return nullptr;
    }
    
    return std::move(std::get<std::unique_ptr<T>>(ast));
  }
  
  void verify_column_ref(const Column_ref* col, 
                        const std::string& name,
                        const std::optional<std::string>& table = std::nullopt,
                        const std::optional<std::string>& alias = std::nullopt) {
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->m_column_name, name);
    EXPECT_EQ(col->m_table_name, table);
    EXPECT_EQ(col->m_alias, alias);
  }

  void verify_table_ref(const Base_table_ref* ref,
                       const std::string& name,
                       const std::optional<std::string>& schema = std::nullopt,
                       const std::optional<std::string>& alias = std::nullopt) {
    ASSERT_NE(ref, nullptr);
    EXPECT_EQ(ref->m_table_name, name);
    EXPECT_EQ(ref->m_schema_name, schema);
    EXPECT_EQ(ref->m_alias, alias);
  }

  void verify_join(const Join* join,
                  Join_type type,
                  bool natural = false) {
    ASSERT_NE(join, nullptr);
    EXPECT_EQ(join->m_type, type);
    EXPECT_EQ(join->m_natural, natural);
    ASSERT_NE(join->m_left, nullptr);
    ASSERT_NE(join->m_right, nullptr);
  }
};

// Basic SELECT tests
TEST_F(Select_test, SimpleSelect) {
  auto stmt = parse_sql<Select_stmt>("SELECT id, name FROM users");
  ASSERT_NE(stmt, nullptr);
  
  // Verify columns
  ASSERT_EQ(stmt->m_columns.size(), 2);
  auto col1 = dynamic_cast<Column_ref*>(stmt->m_columns[0].get());
  auto col2 = dynamic_cast<Column_ref*>(stmt->m_columns[1].get());
  verify_column_ref(col1, "id");
  verify_column_ref(col2, "name");
  
  // Verify FROM clause
  ASSERT_EQ(stmt->m_from.size(), 1);
  auto table = dynamic_cast<Base_table_ref*>(stmt->m_from[0].get());
  verify_table_ref(table, "users");
  
  // Verify no other clauses
  EXPECT_FALSE(stmt->m_where);
  EXPECT_FALSE(stmt->m_group_by);
  EXPECT_TRUE(stmt->m_order_by.empty());
  EXPECT_FALSE(stmt->m_limit);
}

TEST_F(Select_test, SelectWithTableAlias) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT u.id, u.name FROM users u"
  );
  ASSERT_NE(stmt, nullptr);
  
  // Verify columns with table qualifier
  ASSERT_EQ(stmt->m_columns.size(), 2);
  auto col1 = dynamic_cast<Column_ref*>(stmt->m_columns[0].get());
  auto col2 = dynamic_cast<Column_ref*>(stmt->m_columns[1].get());
  verify_column_ref(col1, "id", "u");
  verify_column_ref(col2, "name", "u");
  
  // Verify FROM clause with alias
  ASSERT_EQ(stmt->m_from.size(), 1);
  auto table = dynamic_cast<Base_table_ref*>(stmt->m_from[0].get());
  verify_table_ref(table, "users", std::nullopt, "u");
}

TEST_F(Select_test, SelectWithSchemaQualifier) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT id FROM public.users"
  );
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_from.size(), 1);
  auto table = dynamic_cast<Base_table_ref*>(stmt->m_from[0].get());
  verify_table_ref(table, "users", "public");
}

TEST_F(Select_test, SelectWithColumnAliases) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT id AS user_id, name AS user_name FROM users"
  );
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_columns.size(), 2);
  auto col1 = dynamic_cast<Column_ref*>(stmt->m_columns[0].get());
  auto col2 = dynamic_cast<Column_ref*>(stmt->m_columns[1].get());
  verify_column_ref(col1, "id", std::nullopt, "user_id");
  verify_column_ref(col2, "name", std::nullopt, "user_name");
}

TEST_F(Select_test, SelectStar) {
  auto stmt = parse_sql<Select_stmt>("SELECT * FROM users");
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_columns.size(), 1);
  auto col = dynamic_cast<Column_ref*>(stmt->m_columns[0].get());
  verify_column_ref(col, "*");
}

TEST_F(Select_test, SelectTableQualifiedStar) {
  auto stmt = parse_sql<Select_stmt>("SELECT users.* FROM users");
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_columns.size(), 1);
  auto col = dynamic_cast<Column_ref*>(stmt->m_columns[0].get());
  verify_column_ref(col, "*", "users");
}

// JOIN tests
TEST_F(Select_test, SimpleInnerJoin) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT u.id, o.order_id "
    "FROM users u "
    "INNER JOIN orders o ON u.id = o.user_id"
  );
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_from.size(), 1);
  auto join_ref = dynamic_cast<Join_ref*>(stmt->m_from[0].get());
  ASSERT_NE(join_ref, nullptr);
  verify_join(join_ref->m_join.get(), Join_type::inner);
  
  // Verify join condition
  ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Binary_op>>(
    join_ref->m_join->m_condition));
  auto& condition = std::get<std::unique_ptr<Binary_op>>(
    join_ref->m_join->m_condition);
  EXPECT_EQ(condition->m_op, Binary_op::Op_type::EQ);
}

TEST_F(Select_test, LeftJoinUsingClause) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT * FROM users LEFT JOIN orders USING (id)"
  );
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_from.size(), 1);
  auto join_ref = dynamic_cast<Join_ref*>(stmt->m_from[0].get());
  ASSERT_NE(join_ref, nullptr);
  verify_join(join_ref->m_join.get(), Join_type::left);
  
  // Verify USING clause
  ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Using_clause>>(
    join_ref->m_join->m_condition));
  auto& using_clause = std::get<std::unique_ptr<Using_clause>>(
    join_ref->m_join->m_condition);
  ASSERT_EQ(using_clause->m_columns.size(), 1);
  EXPECT_EQ(using_clause->m_columns[0], "id");
}

TEST_F(Select_test, NaturalJoin) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT * FROM users NATURAL JOIN orders"
  );
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_from.size(), 1);
  auto join_ref = dynamic_cast<Join_ref*>(stmt->m_from[0].get());
  ASSERT_NE(join_ref, nullptr);
  verify_join(join_ref->m_join.get(), Join_type::inner, true);
}

TEST_F(Select_test, MultipleJoins) {
  auto stmt = parse_sql<Select_stmt>(
    "SELECT * FROM orders o "
    "JOIN users u ON o.user_id = u.id "
    "LEFT JOIN items i ON o.item_id = i.id"
  );
  ASSERT_NE(stmt, nullptr);
  
  ASSERT_EQ(stmt->m_from.size(), 1);
  const auto join_ref = dynamic_cast<Join_ref*>(stmt->m_from[0].get());
  ASSERT_NE(join_ref, nullptr);

  // FIXME: Get rid of this extra level of indirection in the grammar
  // Join_ref::Join{Join_ref::Join::m_left, _}
  {
    auto verify_join_ref = static_cast<Join_ref*>(join_ref->m_join->m_left.get());
    ASSERT_NE(verify_join_ref, nullptr);
    // First join (INNER JOIN users)
    verify_join(verify_join_ref->m_join.get(), Join_type::inner);
  }

  {
    // Second join (LEFT JOIN items)
    verify_join(join_ref->m_join.get(), Join_type::left);
  }
}

// Error cases
TEST_F(Select_test, ErrorMissingFrom) {
  EXPECT_THROW(
    parse_sql<Select_stmt>("SELECT id"),
    std::runtime_error
  );
}

TEST_F(Select_test, ErrorInvalidJoinSyntax) {
  EXPECT_THROW(
    parse_sql<Select_stmt>("SELECT * FROM users INNER orders"),
    std::runtime_error
  );
}

TEST_F(Select_test, ErrorMissingJoinCondition) {
  EXPECT_THROW(
    parse_sql<Select_stmt>("SELECT * FROM users JOIN orders"),
    std::runtime_error
  );
}

TEST_F(Select_test, ErrorEmptyUsingClause) {
  EXPECT_THROW(
    parse_sql<Select_stmt>("SELECT * FROM users JOIN orders USING ()"),
    std::runtime_error
  );
}

} // namespace sql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

