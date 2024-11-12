#include <cassert>
#include <memory>

#include "sql/parser.h"
#include "sql/ast.h"

void token_handle() {
  // TODO: Advance to next token
}

void test_lexer() {
    sql::Lexer lexer("SELECT id FROM users WHERE age > 18");
    
    auto token = lexer.next_token();
    const auto lexeme = token.get_lexeme();
    
    assert(lexeme.m_type == sql::Lexeme_type::keyword);
    assert(lexeme.m_value == "SELECT");
    
    /* Advance to next token */
    token_handle();

    assert(lexeme.m_type == sql::Lexeme_type::identifier);
    assert(lexeme.m_value == "id");
}

void test_parser() {
    sql::Lexer lexer("SELECT id FROM users");
    sql::Parser parser(lexer);
    
    auto ast = parser.parse();
    assert(ast != nullptr);
    
    if (auto* select = std::get_if<sql::Select_stmt>(ast.get())) {
        /* TODO: Verify select statement structure */
    }
}

void test_ast() {
    /* Building a SELECT statement AST */
    auto select = std::make_unique<sql::Select_stmt>();

    select->m_distinct = true;

    /* SELECT id, name */
    auto id_col = std::make_unique<sql::Column_ref>();

    id_col->m_column_name = "id";
    select->m_columns.push_back(std::move(id_col));

    auto name_col = std::make_unique<sql::Column_ref>();

    name_col->m_column_name = "name";
    select->m_columns.push_back(std::move(name_col));

    /* FROM users */
    auto users_table = std::make_unique<sql::Table_ref>();

    users_table->m_table_name = "users";
    select->m_from.push_back(std::move(users_table));

    /* WHERE age >= 18 */
    auto where = std::make_unique<sql::Where_clause>();
    auto condition = std::make_unique<sql::Binary_op>();

    condition->m_op = sql::Binary_op::Op_type::gte;

    auto age_col = std::make_unique<sql::Column_ref>();

    age_col->m_column_name = "age";
    condition->m_left = std::move(age_col);

    auto age_value = std::make_unique<sql::Literal>();

    age_value->m_type = sql::Literal::Type::integer;
    age_value->m_value = "18";
    condition->m_right = std::move(age_value);

    where->m_condition = std::move(condition);
    select->m_where = std::move(where);

    /* CREATE INDEX */
    auto create_idx = std::make_unique<sql::Create_index_stmt>();

    create_idx->m_index_name = "idx_user_email";
    create_idx->m_table_name = "users";
    create_idx->m_unique = true;

    sql::Create_index_stmt::Index_column email_col;

    email_col.m_column_name = "email";
    email_col.m_ascending = true;
    create_idx->m_columns.push_back(std::move(email_col));

    // ALTER TABLE example
    auto alter = std::make_unique<sql::Alter_table_stmt>();

    alter->m_table_name = "users";
    alter->m_alter_type = sql::Alter_type::add_constraint;

    sql::Alter_table_stmt::Add_constraint fk;

    fk.m_type = sql::Alter_table_stmt::Add_constraint::Type::foreign_key;
    fk.m_name = "fk_user_dept";
    fk.m_columns = {"department_id"};
    fk.m_foreign_key_def = {{
        .m_table = "departments",
        .m_columns = {"id"},
        .m_on_delete = "CASCADE",
        .m_on_update = "CASCADE"
    }};

    alter->m_alteration = std::move(fk);
}

int main() {
    test_lexer();
    test_parser();
    
    // Example usage
    sql::Lexer lexer("SELECT * FROM users WHERE age >= 21 ORDER BY name");
    sql::Parser parser(lexer);
    
    // auto ast = parser.parse();

    test_ast();

    return 0;
}

