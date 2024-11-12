#include <gtest/gtest.h>
#include <memory>

#include "sql/lexer.h"

namespace sql::mysql::test {

class Event_test : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(Event_test, ParsesEventDefinition) {
  sql::Lexer lexer(
    "CREATE EVENT daily_cleanup "
    "ON SCHEDULE EVERY 1 DAY "
    "DO DELETE FROM temp_records WHERE created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)"
  );
  sql::mysql::Parser parser(lexer);
  
  auto ast = parser.parse();
  ASSERT_NE(ast, nullptr);
  
  auto* event = std::get_if<Create_event_stmt>(ast.get());
  ASSERT_NE(event, nullptr);
  
  EXPECT_EQ(event->m_name, "daily_cleanup");
  EXPECT_EQ(event->m_schedule.m_type, Create_event_stmt::Schedule::Type::every);
  
  const auto& interval = std::get<Create_event_stmt::Schedule::Interval>(
      event->m_schedule.m_value);
  EXPECT_EQ(interval.m_value, 1);
  EXPECT_EQ(interval.m_unit, Create_event_stmt::Schedule::Interval::Unit::day);
}

class Procedure_test : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(Procedure_test, ParsesProcedureDefinition) {
  sql::Lexer lexer(
    "CREATE PROCEDURE calculate_stats("
    " IN start_date DATE,"
    " OUT total_count INT"
    ")"
    "BEGIN"
    " DECLARE temp_count INT DEFAULT 0;"
    " SELECT COUNT(*) INTO temp_count FROM orders"
    " WHERE order_date >= start_date;"
    " SET total_count = temp_count;"
    "END"
  );
  sql::mysql::Parser parser(lexer);
  
  auto ast = parser.parse();
  ASSERT_NE(ast, nullptr);
  
  auto* proc = std::get_if<Create_procedure_stmt>(ast.get());
  ASSERT_NE(proc, nullptr);
  
  EXPECT_EQ(proc->m_name, "calculate_stats");
  ASSERT_EQ(proc->m_body.m_parameters.size(), 2);
  
  const auto& param1 = proc->m_body.m_parameters[0];
  EXPECT_EQ(param1.m_name, "start_date");
  EXPECT_EQ(param1.m_direction, Stored_program_body::Parameter_direction::in_);
}

class Trigger_test : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(Trigger_test, ParsesTriggerDefinition) {
  sql::Lexer lexer(
    "CREATE TRIGGER before_user_update "
    "BEFORE UPDATE ON users "
    "FOR EACH ROW "
    "BEGIN "
    " IF NEW.status != OLD.status THEN "
    "   INSERT INTO user_audit(user_id, old_status, new_status) "
    "   VALUES (NEW.id, OLD.status, NEW.status); "
    " END IF; "
    "END"
  );
  sql::mysql::Parser parser(lexer);
  
  auto ast = parser.parse();
  ASSERT_NE(ast, nullptr);
  
  auto* trigger = std::get_if<Create_trigger_stmt>(ast.get());
  ASSERT_NE(trigger, nullptr);
  
  EXPECT_EQ(trigger->m_name, "before_user_update");
  EXPECT_EQ(trigger->m_timing, Create_trigger_stmt::Timing::before);
  EXPECT_EQ(trigger->m_event, Create_trigger_stmt::Event::update);
  EXPECT_EQ(trigger->m_table_name, "users");
}

} // namespace sql::mysql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

