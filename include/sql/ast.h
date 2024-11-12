#pragma once

#include <string>
#include <vector>
#include <variant>
#include <memory>
#include <optional>
#include <string_view>

#include "sql/lexer.h"

namespace sql {

struct Join;
struct Binary_op;
struct Select_stmt;
struct Using_clause;

/**
 * @brief Base class for all AST nodes
 */
struct Ast_base {
  virtual ~Ast_base() = default;
  virtual std::string to_string() const noexcept = 0;
};

enum class Object_type {
  table,
  index,
  view,
  sequence,
  trigger,
  procedure,
  function,
  database
};

std::string to_string(Object_type type) noexcept;

/**
 * @brief Represents a column data type
 */
struct Data_type {
  std::string to_string() const noexcept;

  enum class Type {
    integer,
    bigint,
    smallint,
    decimal,
    numeric,
    float_,
    double_,
    char_,
    varchar,
    text,
    date,
    time,
    timestamp,
    boolean,
    blob,
    json
  };

  Type m_base_type;

  /* For char/varchar */  
  std::optional<size_t> m_length;

  /* For decimal/numeric */
  std::optional<size_t> m_precision;
  std::optional<size_t> m_scale;

  /* For character types */
  std::optional<std::string> m_charset;
  std::optional<std::string> m_collation;
};

std::string to_string(Data_type::Type type) noexcept;

/**
 * @brief Represents literal values in SQL
 */
struct Literal : Ast_base {
  std::string to_string() const noexcept override;

  enum class Type {
    null,
    integer,
    floating,
    string,
    boolean
  };

  Type m_type;
  std::string m_value;
};

/**
 * @brief Column reference with optional table qualifier
 */
struct Column_ref : Ast_base {
  std::string to_string() const noexcept override;

  std::optional<std::string> m_table_name;
  std::string m_column_name;
  std::optional<std::string> m_alias;
};

/**
 * @brief Base class for table references
 */
struct Table_ref : Ast_base {
  virtual std::string to_string() const noexcept override = 0;

  std::optional<std::string> m_alias;
};

/**
 * @brief Simple table reference
 */
struct Base_table_ref : Table_ref {
  std::string to_string() const noexcept override;

  std::optional<std::string> m_schema_name;
  std::string m_table_name;
};

/**
 * @brief Subquery as a table reference
 */
struct Derived_table_ref : Table_ref {
  std::string to_string() const noexcept override;

  std::unique_ptr<Select_stmt> m_subquery;
};

/**
 * @brief USING clause for joins
 */
struct Using_clause : Ast_base {
  std::string to_string() const noexcept override;

  std::vector<std::string> m_columns;
};

// FIXME: Get rid of this extra level of indirection in the grammar
// Join_ref::Join{Join_ref::Join::m_left, Join_ref::Join::m_right}
struct Join_ref : Table_ref {
  std::string to_string() const noexcept override;

  std::unique_ptr<Join> m_join;
};

/**
 * @brief Join types supported in FROM clause
 * 
 * Handle queries like:
 * 
 * SELECT * FROM t1, t2
 * SELECT * FROM t1 JOIN t2 ON t1.id = t2.id
 * SELECT * FROM t1 NATURAL JOIN t2
 * SELECT * FROM t1 LEFT JOIN t2 USING (id)
 * SELECT * FROM (SELECT * FROM t1) AS subq
 */
enum class Join_type {
  inner,
  left,
  right,
  full,
  cross
};

std::string to_string(Join_type type) noexcept;

/**
 * @brief Represents a JOIN operation in FROM clause
 */
struct Join : Ast_base {
  std::string to_string() const noexcept override;

  Join_type m_type;
  std::unique_ptr<Table_ref> m_left;
  std::unique_ptr<Table_ref> m_right;
  bool m_natural{false};
  /* ON clause or USING columns */
  std::variant<
    /* ON condition */  
    std::unique_ptr<Binary_op>,
    /* USING clause */
    std::unique_ptr<Using_clause>
  > m_condition;
};


/**
 * @brief Binary operation (e.g., arithmetic, comparison)
 */
struct Binary_op : Ast_base {
  std::string to_string() const noexcept override;

  /* Note: if you add a new operator here, don't forget to
   * update Parser::parse_operator() in parser.cc. We can't
   * use a switch there because we have to compare strings. */
  enum class Op_type {
    EQ,        // =
    NEQ,       // <>
    LT,        // <
    GT,        // >
    LTE,       // <=
    GTE,       // >=
    ADD,       // +
    SUBTRACT,  // -
    MULTIPLY,  // *
    DIVIDE,    // /
    MOD,       // %
    AND,       // AND
    OR,        // OR
    LIKE,      // LIKE
    IN,        // IN
    COMMA      // ,
  };

  Op_type m_op;
  std::unique_ptr<Ast_base> m_left;
  std::unique_ptr<Ast_base> m_right;
};

std::string to_string(Binary_op::Op_type op) noexcept;

/**
 * @brief Represents a foreign key reference
 */
struct Foreign_key_reference {
  std::string to_string() const noexcept;

  std::string m_table_name;
  std::vector<std::string> m_column_names;
    
  enum class Match_type {
    simple,
    full,
    partial
  };

  std::optional<Match_type> m_match;

  enum class Action {
    no_action,
    restrict,
    cascade,
    set_null,
    set_default
  };

  std::optional<Action> m_on_delete;
  std::optional<Action> m_on_update;
    
  /* ENFORCED or NOT ENFORCED */
  std::optional<bool> m_enforced;
};

std::string to_string(Foreign_key_reference::Match_type match) noexcept;
std::string to_string(Foreign_key_reference::Action action) noexcept;

/**
 * @brief Unary operation (e.g., NOT, EXISTS)
 */
struct Unary_op : Ast_base {
  enum class Op_type {
    NOT,
    EXISTS,
    IS_NULL,
    IS_NOT_NULL
  };

  Op_type m_op;
  std::unique_ptr<Ast_base> m_operand;
};

/**
 * @brief ORDER BY clause item
 */
struct Order_by_item : Ast_base {
  std::string to_string() const noexcept override;

  std::unique_ptr<Column_ref> m_column;
  bool m_ascending{true};
  /* NULLS FIRST/LAST */
  std::optional<std::string> m_nulls;
};

/**
 * @brief GROUP BY clause
 */
struct Group_by : Ast_base {
  std::string to_string() const noexcept override;

  std::vector<std::unique_ptr<Column_ref>> m_columns;
  std::unique_ptr<Ast_base> m_having;
};

/**
 * @brief CASE expression
 */
struct Case_expr : Ast_base {
  struct When_then {
    std::unique_ptr<Ast_base> m_when;
    std::unique_ptr<Ast_base> m_then;
  };

  std::unique_ptr<Ast_base> m_case_expr;
  std::vector<When_then> m_when_then_pairs;
  std::unique_ptr<Ast_base> m_else;
};

/**
 * @brief WHERE clause
 */
struct Where_clause : Ast_base {
  std::string to_string() const noexcept override;

  std::unique_ptr<Ast_base> m_condition;

  [[nodiscard]] bool has_value() const noexcept {
    return m_condition != nullptr;
  }
};

/**
 * @brief Represents a subquery
 */
struct Subquery : Ast_base {
  std::unique_ptr<Ast_base> m_query;
  std::optional<std::string> m_alias;
};

/**
 * @brief Common Table Expression (WITH clause)
 */
struct Cte : Ast_base {
  std::string m_name;
  std::vector<std::string> m_column_names;
  std::unique_ptr<Ast_base> m_query;
};

/**
 * @brief SELECT statement
 */
struct Select_stmt : Ast_base {
  std::string to_string() const noexcept override;

  bool m_distinct{false};
  /* Can be Column_ref or Function_call */
  std::vector<std::unique_ptr<Ast_base>> m_columns;
  std::vector<std::unique_ptr<Table_ref>> m_from;
  std::vector<std::unique_ptr<Join>> m_joins;
  std::unique_ptr<Where_clause> m_where;
  std::unique_ptr<Group_by> m_group_by;
  std::vector<std::unique_ptr<Order_by_item>> m_order_by;
  std::optional<size_t> m_limit;
  std::optional<size_t> m_offset;
  /* WITH clause */
  std::vector<std::unique_ptr<Cte>> m_with;
};

struct Column_def : Ast_base {
  std::string to_string() const noexcept override;

  std::string m_name;
  Data_type m_type;
  bool m_nullable{true};
  bool m_primary_key{false};
  bool m_unique{false};
  bool m_auto_increment{false};
  std::unique_ptr<Ast_base> m_default_value;
  std::unique_ptr<Ast_base> m_check;
  std::unique_ptr<Foreign_key_reference> m_references;
  std::optional<std::string> m_comment;
  std::optional<std::string> m_collation;

  /* Storage options */
  /* STORED/VIRTUAL for generated columns */
  std::optional<std::string> m_storage;
  /* For generated columns */
  std::unique_ptr<Ast_base> m_generation_expr;
};

/**
 * @brief CREATE TABLE statement
 */
struct Create_table_stmt : Ast_base {
  std::string m_table_name;
  bool m_if_not_exists{false};
  std::vector<Column_def> m_columns;
};

/**
* @brief ALTER TABLE statement types
*/
enum class Alter_type {
  add_column,
  drop_column,
  modify_column,
  add_constraint,
  drop_constraint,
  rename_column,
  rename_table
};

std::string to_string(Alter_type type) noexcept;

/**
 * @brief ALTER TABLE statement
 */
struct Alter_table_stmt : Ast_base {
  std::string to_string() const noexcept override;

  struct Add_column {
    std::string to_string() const noexcept;

    Column_def m_column{};
    std::optional<std::string> m_after_column{};
    bool m_first{};
  };

  struct Modify_column {
    std::string to_string() const noexcept;

    std::string m_column_name{};
    Column_def m_new_definition{};
  };

  struct Rename_column {
    std::string to_string() const noexcept;

    std::string m_old_name{};
    std::string m_new_name{};
  };

  struct Add_constraint {
    std::string to_string() const noexcept;

    enum class Type {
      primary_key,
      foreign_key,
      unique,
      check
    };

    Type m_type{};
    std::optional<std::string> m_name{};
    std::vector<std::string> m_columns{};

    struct Foreign_key_def {
      std::string to_string() const noexcept;

      std::string m_table{};
      std::vector<std::string> m_columns{};
      std::string m_on_delete{};
      std::string m_on_update{};
    };

    /* For foreign key */
    std::optional<Foreign_key_def> m_foreign_key_def{};

    /* For check constraint */
    std::unique_ptr<Ast_base> m_check_condition{};
  };

  std::string m_table_name{};

  Alter_type m_alter_type{};

  struct Drop_column {
    std::string to_string() const noexcept;

    bool m_cascade{};
    std::string m_column_name{};
  };

  struct Drop_constraint {
    std::string to_string() const noexcept;

    bool m_cascade{};
    std::string m_constraint{};
  };

  struct Rename_table {
    std::string to_string() const noexcept;

    /** New table name. */
    std::string m_name{};
  };

  using Alteration = std::variant<
    Add_column,
    Drop_column,
    Modify_column,
    Add_constraint,
    Drop_constraint,
    Rename_column,
    Rename_table 
  >;

  Alteration  m_alteration{std::in_place_type<Rename_table>, ""};

  /* If exists specified */
  bool m_if_exists{};

  /* If all inheritance specified */
  bool m_all_inheritance{};

  /* If only specified */
  bool m_only{}; 
};

std::string to_string(const Alter_table_stmt::Alteration& alteration) noexcept;
std::string to_string(Alter_table_stmt::Add_constraint::Type type) noexcept;

/**
 * @brief Base class for all ALTER statement types (more generic).
 */
struct Alter_stmt : Ast_base {
  std::string to_string() const noexcept override;

  Object_type m_object_type;
  bool m_if_exists{false};
  
  /* The actual definition is stored in one of these */
  std::variant<
    // FIXME: std::unique_ptr<Alter_view_stmt>,
    std::unique_ptr<Alter_table_stmt>
  > m_definition;
};

/**
* @brief CREATE INDEX statement
*/
struct Create_index_stmt : Ast_base {
  struct Index_column {
    std::string m_column_name;
    bool m_ascending{true};
    /* For partial indexes */
    std::optional<size_t> m_length;
    std::optional<std::string> m_collation;
  };

  std::string m_index_name;
  std::string m_table_name;
  std::vector<Index_column> m_columns;
  bool m_unique{false};
  bool m_if_not_exists{false};

  /* Index type (e.g., BTREE, HASH) */
  std::optional<std::string> m_index_type;

  /* Index options */
  struct {
    std::optional<std::string> m_algorithm;
    std::optional<std::string> m_lock;
  } m_options;
};

/**
 * @brief DROP statement (TABLE, INDEX, etc.)
 */
struct Drop_stmt : Ast_base {
  std::string to_string() const noexcept override;

  bool m_cascade{false};
  bool m_if_exists{false};
  Object_type m_object_type;
  std::vector<std::string> m_object_names;
};

/**
 * @brief TRUNCATE TABLE statement
 */
struct Truncate_stmt : Ast_base {
  std::string m_table_name;
};

/**
 * @brief CREATE VIEW statement
 */
struct Create_view_stmt : Ast_base {
  std::string m_view_name;
  std::vector<std::string> m_column_names;
  std::unique_ptr<Select_stmt> m_select;
  bool m_or_replace{false};
  bool m_materialized{false};
  /* UNDEFINED, MERGE, TEMPTABLE */
  std::optional<std::string> m_algorithm;
  /* DEFINER, INVOKER */
  std::optional<std::string> m_security;
  /* LOCAL, CASCADED */
  std::optional<std::string> m_check_option;
};

/**
 * @brief CREATE TRIGGER statement
 */
struct Create_trigger_stmt : Ast_base {
  // std::string to_string() const noexcept override;

  enum class Timing {
    before,
    after,
    instead_of
  };

  enum class Event {
    insert,
    update,
    delete_
  };

  Event m_event;
  Timing m_timing;
  std::string m_table_name;
  bool m_or_replace{false};
  std::string m_trigger_name;

  struct {
    bool m_for_each_row{false};
    std::unique_ptr<Ast_base> m_when_condition;
  } m_options;

  /* List of statements */
  std::vector<std::unique_ptr<Ast_base>> m_body;
};

/**
 * @brief CREATE PROCEDURE/FUNCTION statement
 */
struct Create_routine_stmt : Ast_base {
  struct Parameter {
    enum class Mode {
      sql_in,
      out,
      inout
    };

    std::string m_type;
    std::string m_name;
    /* Not used for functions */
    std::optional<Mode> m_mode;
  };

  enum class Routine_type {
    procedure,
    function
  };

  Routine_type m_routine_type;
  std::string m_name;
  std::vector<Parameter> m_parameters;
  /* For functions */
  std::optional<std::string> m_return_type;
  std::vector<std::unique_ptr<Ast_base>> m_body;
  bool m_or_replace{false};

  struct {
    /* SQL, JAVASCRIPT, etc. */
    std::optional<std::string> m_language;
    /* DEFINER, INVOKER */
    std::optional<std::string> m_security;
    std::optional<bool> m_deterministic;
    /* CONTAINS SQL, NO SQL, etc. */
    std::optional<std::string> m_sql_data_access;
  } m_characteristics;
};

/**
 * @brief GRANT/REVOKE statement
 */
struct Grant_revoke_stmt : Ast_base {
  enum class Operation {
    grant,
    revoke
  };

  struct Privilege {
    enum class Type {
      select,
      insert,
      update,
      delete_,
      truncate,
      references,
      trigger,
      all
    };

    Type m_type;
    /* For column-level privileges */
    std::vector<std::string> m_columns;
  };

  Operation m_operation;
  std::vector<Privilege> m_privileges;

  enum class Object_type {
    table,
    view,
    procedure,
    function,
    database
  };

  Object_type m_object_type;
  std::vector<std::string> m_object_names;

  /* Users/roles */
  std::vector<std::string> m_grantees;

  /* For GRANT */
  bool m_with_grant_option{false};

  /* For REVOKE */
  bool m_cascade{false};
};

std::string to_string(Grant_revoke_stmt::Object_type type) noexcept;
std::string to_string(Grant_revoke_stmt::Privilege::Type type) noexcept;

/**
 * @brief UPDATE statement
 */
struct Update_stmt : Ast_base {
  std::string to_string() const noexcept override;

  struct Assignment {
    std::string m_column;
    std::unique_ptr<Ast_base> m_value;
  };

  std::unique_ptr<Table_ref> m_table;
  std::vector<Assignment> m_assignments;
  std::unique_ptr<Where_clause> m_where;
  std::vector<std::unique_ptr<Order_by_item>> m_order_by;
  std::optional<size_t> m_limit;
};

/**
 * @brief INSERT statement
 */
struct Insert_stmt : Ast_base {
  std::string to_string() const noexcept override;

  std::string m_table_name;
  std::vector<std::string> m_columns;

  /* Either VALUES lists or SELECT statement */
  std::variant<
    /* Values */
    std::vector<std::vector<std::unique_ptr<Ast_base>>>,
    /* Select */
    std::unique_ptr<Select_stmt>
  > m_values;

  /* Optional ON DUPLICATE KEY UPDATE */
  std::optional<std::vector<Update_stmt::Assignment>> m_on_duplicate;
};

/**
 * @brief DELETE statement
 */
struct Delete_stmt : Ast_base {
  std::string to_string() const noexcept override;

  std::unique_ptr<Table_ref> m_table;
  std::optional<std::vector<std::unique_ptr<Table_ref>>> m_using;
  std::unique_ptr<Where_clause> m_where;
  std::vector<std::unique_ptr<Order_by_item>> m_order_by;
  std::optional<size_t> m_limit;
};

enum class Column_position {
  default_,
  first,
  after
};

struct Alter_action : Ast_base {
  virtual ~Alter_action() = default;
};

struct Add_column_action : Alter_action {
  std::unique_ptr<Column_def> m_column;
  Column_position m_position{Column_position::default_};
  std::optional<std::string> m_after_column;
};




/**
 * @brief Represents a table constraint
 */
struct Table_constraint : Ast_base {
  std::string to_string() const noexcept override;

  enum class Type {
    primary_key,
    foreign_key,
    unique,
    check
  };

  std::optional<std::string> m_name;  // Constraint name
  Type m_type;

  /* Columns for PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints */
  std::vector<std::string> m_columns;

  /* For FOREIGN KEY constraints */
  std::unique_ptr<Foreign_key_reference> m_reference;

  /* For CHECK constraints */
  std::unique_ptr<Ast_base> m_check;

  /* Common options */
  bool m_deferrable{false};
  enum class Initially {
    immediate,
    deferred
  };
  std::optional<Initially> m_initially;
};

std::string to_string(Table_constraint::Type type) noexcept;
std::string to_string(Table_constraint::Initially initially) noexcept;

/**
 * @brief Represents a table index
 */
struct Index_column {
  std::optional<std::string> m_column_name;

  /* For expression indexes */
  std::unique_ptr<Ast_base> m_expression;

  /* Prefix length for string columns */
  std::optional<size_t> m_length;

  /* Sort direction */
  bool m_ascending{true};

  /* Custom collation */
  std::optional<std::string> m_collation;
};

/**
 * @brief Represents a column definition
 */
/**
 * @brief Represents a CREATE TABLE definition
 */
struct Create_table_def : Ast_base {
  std::string to_string() const noexcept override;

  std::string m_table_name;
  std::vector<std::unique_ptr<Column_def>> m_columns;
  std::vector<std::unique_ptr<Table_constraint>> m_constraints;

  /* Table options */
  struct Options {
    std::optional<std::string> m_engine;
    std::optional<std::string> m_charset;
    std::optional<std::string> m_collate;
    std::optional<size_t> m_auto_increment;
    std::optional<std::string> m_comment;
    std::optional<size_t> m_avg_row_length;
    std::optional<size_t> m_max_rows;
    std::optional<size_t> m_min_rows;
    std::optional<std::string> m_row_format;
    std::optional<size_t> m_key_block_size;
    std::optional<std::string> m_data_directory;
    std::optional<std::string> m_index_directory;
    std::optional<std::string> m_tablespace;
    std::optional<std::string> m_compression;
  } m_options;

  /* Partitioning options */
  struct Partition {
    std::string to_string() const;

    enum class Type {
      range,
      list,
      hash,
      key
    };

    std::optional<Type> m_type;
    std::vector<std::string> m_columns;
    size_t m_partitions{1};

    /* For HASH/KEY */
    std::unique_ptr<Ast_base> m_expression;

    struct Range_value {
      std::unique_ptr<Ast_base> m_value;
      std::optional<std::string> m_name;
      std::optional<std::string> m_engine;
      std::optional<std::string> m_comment;
      std::optional<size_t> m_max_rows;
      std::optional<std::string> m_data_directory;
      std::optional<std::string> m_index_directory;
    };

    /* For RANGE/LIST */
    std::vector<Range_value> m_values;
  } m_partition;
};

std::string to_string(Create_table_def::Partition::Type type) noexcept;

/**
 * @brief Represents a CREATE INDEX definition
 */
struct Create_index_def : Ast_base {
  std::string to_string() const noexcept override;

  std::string m_index_name;
  std::string m_table_name;
  std::vector<Index_column> m_columns;
  bool m_unique{false};

  /* Index options */
  /* Btree, Hash, etc. */
  std::optional<std::string> m_index_type;
  std::optional<std::string> m_comment;
  std::optional<size_t> m_key_block_size;
  std::optional<bool> m_visible;

  struct Algorithm {
    enum class Type {
      default_,
      inplace,
      copy
    };
    Type m_type{Type::default_};
  } m_algorithm;

  struct Lock {
    enum class Type {
      default_,
      none,
      shared,
      exclusive
    };
    Type m_type{Type::default_};
  } m_lock;
};

/**
 * @brief Represents a CREATE VIEW definition
 */
struct Create_view_def : Ast_base {
  std::string to_string() const noexcept override;

  std::string m_view_name;
  std::vector<std::string> m_column_names;
  std::unique_ptr<Select_stmt> m_select;
  bool m_with_check_option{false};
    
  enum class Algorithm {
    undefined,
    merge,
    temptable
  };
  std::optional<Algorithm> m_algorithm;
    
  enum class Security {
    definer,
    invoker
  };
  std::optional<Security> m_sql_security;
    
  enum class Check_option {
    local,
    cascaded
  };
  std::optional<Check_option> m_check_option;
    
  bool m_or_replace{false};

  /* User who owns the view */
  std::optional<std::string> m_definer;
};

std::string to_string(Create_view_def::Algorithm algorithm) noexcept;
std::string to_string(Create_view_def::Security security) noexcept;
std::string to_string(Create_view_def::Check_option check_option) noexcept;

/**
 * @brief Represents a CREATE SEQUENCE definition
 */
struct Create_sequence_def : Ast_base {
  std::string m_sequence_name;
    
  std::optional<int64_t> m_start_value;
  std::optional<int64_t> m_increment;
  std::optional<int64_t> m_min_value;
  std::optional<int64_t> m_max_value;
  std::optional<int64_t> m_cache;
  bool m_cycle{false};
    
  enum class Order {
    ordered,
    unordered
  };
  std::optional<Order> m_order;
};

/**
 * @brief Represents a CREATE TRIGGER definition
 */
struct Create_trigger_def : Ast_base {
  std::string m_trigger_name;
  std::string m_table_name;
    
  enum class Timing {
    before,
    after,
    instead_of
  };
  Timing m_timing;
    
  enum class Event {
    insert,
    update,
    delete_
  };
  Event m_event;
    
  /* For UPDATE triggers, which columns trigger it */
  std::optional<std::vector<std::string>> m_update_columns;
    
  bool m_for_each_row{false};  // Row-level vs Statement-level
    
  /* Optional WHEN condition */
  std::unique_ptr<Ast_base> m_when_condition;
    
  /* Trigger body contains multiple statements */
  std::vector<std::unique_ptr<Ast_base>> m_body;
    
  /* Order between triggers */
  struct Order {
    bool m_follows{false};  // true = FOLLOWS, false = PRECEDES
    std::string m_other_trigger_name;
  };
  std::optional<Order> m_order;
    
  /* User who owns the trigger */
  std::optional<std::string> m_definer;
};

/**
 * @brief Parameter definition for stored procedures/functions
 */
struct Parameter_def : Ast_base {
  enum class Direction {
    sql_in,
    out,
    inout
  };

  /* Not used for functions */
  std::optional<Direction> m_direction;
    
  std::string m_name;
  Data_type m_type;
    
  /* Optional default value */
  std::unique_ptr<Ast_base> m_default_value;
};

/**
 * @brief Common elements for procedures and functions
 */
struct Stored_program : Ast_base {
  std::string m_name;
  std::vector<Parameter_def> m_parameters;
    
  /* Characteristics */
  enum class Data_access {
    contains_sql,
    no_sql,
    reads_sql_data,
    modifies_sql_data
  };
  std::optional<Data_access> m_sql_data_access;
    
  enum class Security {
    definer,
    invoker
  };
  std::optional<Security> m_sql_security;
    
  std::optional<bool> m_deterministic;
  std::optional<std::string> m_comment;
  std::optional<std::string> m_definer;
    
  /* Local declarations */
  struct Variable_declaration {
    std::string m_name;
    Data_type m_type;
    std::unique_ptr<Ast_base> m_default_value;
  };
  std::vector<Variable_declaration> m_variables;
    
    struct Condition_declaration {
    std::string m_name;
    std::variant<
      /* MySQL error name */
      std::string,
      /* MySQL error code */
      int,
      /* SQLSTATE value */
      std::string
    > m_value;
  };
  std::vector<Condition_declaration> m_conditions;
    
  struct Cursor_declaration {
    std::string m_name;
    std::unique_ptr<Select_stmt> m_select;
  };
  std::vector<Cursor_declaration> m_cursors;
    
  struct Handler_declaration {
    enum class Type {
      continue_,
      exit,
      undo
    };
    Type m_type;
        
    enum class Condition_value_type {
      sqlstate,
      condition_name,
      sqlwarning,
      not_found,
      sqlexception
    };
    Condition_value_type m_condition_value_type;
        
    std::optional<std::string> m_condition_value;
    std::vector<std::unique_ptr<Ast_base>> m_statement_list;
  };
  std::vector<Handler_declaration> m_handlers;
    
  /* Body contains multiple statements */
  std::vector<std::unique_ptr<Ast_base>> m_body;
};

/**
 * @brief Represents a CREATE PROCEDURE definition
 */
struct Create_procedure_def : Stored_program {
  /* Additional procedure-specific properties can be added here */
};

/**
 * @brief Represents a CREATE FUNCTION definition
 */
struct Create_function_def : Stored_program {
  /* Return type for function */
  Data_type m_return_type;
    
  /* UDF-specific properties */
  std::optional<std::string> m_soname;  // Shared library name
};


/**
 * @brief Base class for CREATE statement definitions
 */
struct Create_stmt : Ast_base {
  std::string to_string() const noexcept override;

  enum class Object_type {
    table,
    index,
    view,
    sequence,
    trigger,
    procedure,
    function
  };

  Object_type m_object_type;
  bool m_if_not_exists{false};

  /* For views */
  bool m_or_replace{false};

  /* For tables */
  bool m_temporary{false};

  /* The actual definition is stored in one of these */
  std::variant<
    std::unique_ptr<Create_table_def>,
    std::unique_ptr<Create_index_def>,
    std::unique_ptr<Create_view_def>,
    std::unique_ptr<Create_sequence_def>,
    std::unique_ptr<Create_trigger_def>,
    std::unique_ptr<Create_procedure_def>
  > m_definition;
};

struct Add_constraint_action : Alter_action {
  std::unique_ptr<Table_constraint> m_constraint;
};

struct Drop_column_action : Alter_action {
  std::string m_column_name;
  bool m_cascade{false};
};

struct Drop_constraint_action : Alter_action {
  std::string m_constraint_name;
  bool m_cascade{false};
};

struct Modify_column_action : Alter_action {
  std::unique_ptr<Column_def> m_column;
};

enum class Alter_column_type {
  set_default,
  drop_default,
  set_not_null,
  drop_not_null
};

struct Alter_column_action : Alter_action {
  std::string m_column_name;
  Alter_column_type m_type;
  std::unique_ptr<Ast_base> m_default_value;
};

struct Rename_column_action : Alter_action {
  std::string m_old_name;
  std::string m_new_name;
};

struct Rename_table_action : Alter_action {
  std::string m_new_name;
};

/**
 * @brief MERGE statement (UPSERT)
 */
struct Merge_stmt : Ast_base {
  // std::string to_string() const noexcept override;

  std::unique_ptr<Table_ref> m_target;
  std::unique_ptr<Table_ref> m_source;
  std::unique_ptr<Ast_base> m_condition;

  struct When_clause {
    std::string to_string() const noexcept;

    enum class Match_type {
      matched,
      not_matched_target,
      not_matched_source
    };

    Match_type m_type;
    std::unique_ptr<Ast_base> m_condition;

    std::variant<
      /* UPDATE */
      Update_stmt::Assignment,
      /* INSERT */
      std::pair<std::vector<std::string>, std::vector<std::unique_ptr<Ast_base>>>,
      /* DELETE */
      std::monostate
    > m_action;
  };

  std::vector<When_clause> m_when_clauses;
};

std::string to_string(Merge_stmt::When_clause::Match_type type) noexcept;

struct Window_spec : Ast_base {
  std::string to_string() const noexcept override;

  /* Optional named window reference */
  std::optional<std::string> m_reference_name;
    
  /* PARTITION BY clause */
  struct Partition {
    std::vector<std::unique_ptr<Column_ref>> m_columns;
  };
  std::optional<Partition> m_partition;
    
  /* ORDER BY clause */
  std::vector<std::unique_ptr<Order_by_item>> m_order;
    
  /* Frame clause */
  struct Frame {
    std::string to_string() const noexcept;

    enum class Type {
      rows,
      range,
      groups
    };
        
    struct Bound {
      std::string to_string() const noexcept;

      enum class Type {
        current_row,
        unbounded_preceding,
        unbounded_following,
        preceding,
        following
      };
            
      Type m_type;
      std::unique_ptr<Ast_base> m_offset;
    };
        
    enum class Exclude {
      no_others,
      current_row,
      group,
      ties
    };
        
    Type m_type;
    Bound m_start;
    Bound m_end;
    std::optional<Exclude> m_exclude;
  };

  std::unique_ptr<Frame> m_frame;
};

std::string to_string(Window_spec::Frame::Type type) noexcept;
std::string to_string(Window_spec::Frame::Bound::Type type) noexcept;
std::string to_string(Window_spec::Frame::Exclude exclude) noexcept;

/**
 * @brief Function call with arguments, DISTINCT, COUNT(*), and window specification
 */
struct Function_call : Ast_base {
  std::string to_string() const noexcept override;

  std::string m_function_name;
  std::vector<std::unique_ptr<Ast_base>> m_arguments;
  bool m_distinct{false};
  /* For COUNT(*) */
  bool m_star{false};
  std::unique_ptr<Window_spec> m_window;
};

struct Column_reference {
  std::optional<std::string> m_schema;
  std::optional<std::string> m_table;
  std::string m_column;
  bool m_ascending{true};
  enum class Nulls_position {
    DEFAULT,
    FIRST,
    LAST
  } m_nulls_position{Nulls_position::DEFAULT};
  /* For index prefix lengths   */
  std::optional<size_t> m_length;  
  /* For collations */
  std::optional<std::string> m_collation;
};

} // namespace sql
