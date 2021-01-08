"""
Visitor for SystemManager

Date: 2020/11/30
"""
from datetime import datetime

from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.sql_parser.SQLVisitor import SQLVisitor
from Pybase.utils.tools import to_str, to_int
from Pybase.meta_system import ColumnInfo, TableInfo
from Pybase.exceptions.run_sql import DataBaseError
from .manager import SystemManger
from .result import QueryResult
from .condition import Condition, ConditionType
from Pybase import settings


class SystemVisitor(SQLVisitor):
    """Visitor to finish system and database SQL"""

    def __init__(self, manager=None):
        super().__init__()
        self.manager: SystemManger = manager
        self.last_start = None

    def time_cost(self):
        current = datetime.now()
        cost = (current - self.last_start) if self.last_start else None
        self.last_start = current
        return cost

    def aggregateResult(self, aggregate, next_result):
        return aggregate if next_result is None else next_result

    def visitProgram(self, ctx: SQLParser.ProgramContext):
        results = []
        for statement in ctx.statement():
            try:
                result: QueryResult = statement.accept(self)
                cost = self.time_cost()
                if result:
                    result.cost = cost
                    result.simplify()
                    results.append(result)
            except DataBaseError as e:
                # Once meet error, record result and stop visiting
                results.append(QueryResult(message=str(e), cost=self.time_cost()))
                break
        return results

    def visitSystem_statement(self, ctx: SQLParser.System_statementContext):
        return QueryResult('databases', tuple(self.manager.dbs))

    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        return self.manager.create_db(to_str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        return self.manager.drop_db(to_str(ctx.Identifier()))

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        return self.manager.use_db(to_str(ctx.Identifier()))

    def visitShow_tables(self, ctx: SQLParser.Show_tablesContext):
        return QueryResult('tables', self.manager.show_tables())

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        columns, foreign_keys, primary = ctx.field_list().accept(self)
        table_name = to_str(ctx.Identifier())
        res = self.manager.create_table(TableInfo(table_name, columns))
        for col in foreign_keys:
            self.manager.add_foreign(table_name, col, foreign_keys[col])
        self.manager.set_primary(table_name, primary)
        return res

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        table_name = to_str(ctx.Identifier())
        return self.manager.drop_table(table_name)

    def visitField_list(self, ctx: SQLParser.Field_listContext):
        name_to_column = {}
        foreign_keys = {}
        primary_key = None
        # Modified by Dong: remove reversed
        for field in ctx.field():
            if isinstance(field, SQLParser.Normal_fieldContext):
                name = to_str(field.Identifier())
                type_, size = field.type_().accept(self)
                # not_null = field.getChild(2) == 'NOT'
                # default = to_str(field.value()) if field.value() else None
                name_to_column[name] = ColumnInfo(type=type_,
                                                  name=name,
                                                  size=size)
            elif isinstance(field, SQLParser.Foreign_key_fieldContext):

                field_name, table_name, refer_name = tuple(to_str(each) for each in field.Identifier())
                foreign_keys[field_name] = table_name, refer_name
            else:
                assert isinstance(field, SQLParser.Primary_key_fieldContext)
                names = field.identifiers().accept(self)
                # assert len(names) == 1
                for name in names:
                    assert name in name_to_column
                primary_key = names
        return list(name_to_column.values()), foreign_keys, primary_key

    def visitNormal_field(self, ctx: SQLParser.Normal_fieldContext):
        pass

    def visitForeign_key_field(self, ctx: SQLParser.Foreign_key_fieldContext):
        pass

    def visitPrimary_key_field(self, ctx: SQLParser.Primary_key_fieldContext):
        pass

    def visitIdentifiers(self, ctx: SQLParser.IdentifiersContext):
        return tuple(to_str(each) for each in ctx.Identifier())

    def visitType_(self, ctx: SQLParser.Type_Context):
        type_ = to_str(ctx.getChild(0))
        size = to_int(ctx.Integer()) if ctx.Integer() else 0
        return type_, size

    def visitDescribe_table(self, ctx: SQLParser.Describe_tableContext):
        table_name = to_str(ctx.Identifier())
        return self.manager.describe_table(table_name)

    def visitInsert_into_table(self, ctx: SQLParser.Insert_into_tableContext):
        table_name = to_str(ctx.getChild(2))
        value_lists = ctx.value_lists().accept(self)
        for value_list in value_lists:
            self.manager.insert_record(table_name, value_list)
        return QueryResult('inserted_items', (len(value_lists),))

    def visitValue_lists(self, ctx: SQLParser.Value_listsContext):
        return tuple(each.accept(self) for each in ctx.value_list())

    def visitValue_list(self, ctx: SQLParser.Value_listContext):
        return tuple(to_str(each) for each in ctx.value())

    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        table_names = ctx.identifiers().accept(self)
        conditions = ctx.where_and_clause().accept(self) if ctx.where_and_clause() else []
        return self.manager.select_records(table_names, conditions)

    def visitDelete_from_table(self, ctx: SQLParser.Delete_from_tableContext):
        table_name = to_str(ctx.Identifier())
        conditions = ctx.where_and_clause().accept(self)
        return self.manager.delete_records(table_name, conditions)

    def visitUpdate_table(self, ctx: SQLParser.Update_tableContext):
        table_name = to_str(ctx.Identifier())
        conditions = ctx.where_and_clause().accept(self)
        set_value_map = ctx.set_clause().accept(self)
        return self.manager.update_records(table_name, conditions, set_value_map)

    def visitWhere_and_clause(self, ctx: SQLParser.Where_and_clauseContext):
        return tuple(each.accept(self) for each in ctx.where_clause())

    def visitWhere_operator_expression(self, ctx: SQLParser.Where_operator_expressionContext):
        table_name, column_name = ctx.column().accept(self)
        operator = to_str(ctx.operator())
        if operator == '=':
            operator = '=='
        if operator == '<>':
            operator = "!="
        value = ctx.expression().accept(self)
        if isinstance(value, tuple):
            return Condition(ConditionType.Compare, table_name, column_name, operator,
                             target_table=value[0], target_column=value[1])
        else:
            return Condition(ConditionType.Compare, table_name, column_name, operator, value=value)

    def visitWhere_operator_select(self, ctx: SQLParser.Where_operator_selectContext):
        pass

    def visitWhere_null(self, ctx: SQLParser.Where_nullContext):
        table_name, column_name = ctx.column().accept(self)
        operator = '==' if ctx.getChild(2) == "NOT" else '!='
        return Condition(ConditionType.Compare, table_name, column_name, operator, settings.NULL_VALUE)

    def visitWhere_in_list(self, ctx: SQLParser.Where_in_listContext):
        pass

    def visitWhere_in_select(self, ctx: SQLParser.Where_in_selectContext):
        pass

    def visitWhere_like_string(self, ctx: SQLParser.Where_like_stringContext):
        pattern = to_str(ctx.String())
        table_name, column_name = ctx.column().accept(self)
        return Condition(ConditionType.Like, table_name, column_name, value=pattern)

    def visitColumn(self, ctx: SQLParser.ColumnContext):
        if len(ctx.Identifier()) == 1:
            return None, to_str(ctx.Identifier(0))
        else:
            return to_str(ctx.Identifier(0)), to_str(ctx.Identifier(1))

    def visitExpression(self, ctx: SQLParser.ExpressionContext):
        if ctx.value() is None:
            return ctx.column().accept(self)
        else:
            return to_str(ctx.value())

    def visitSet_clause(self, ctx: SQLParser.Set_clauseContext):
        set_value_map = {}
        for identifier, value in zip(ctx.Identifier(), ctx.value()):
            set_value_map[to_str(identifier)] = to_str(value)
        return set_value_map

    def visitCreate_index(self, ctx: SQLParser.Create_indexContext):
        index_name = to_str(ctx.getChild(2))
        table_name = to_str(ctx.getChild(4))
        col_list = ctx.identifiers().accept(self)
        for colname in col_list:
            self.manager.create_index(index_name, table_name, colname)

    def visitDrop_index(self, ctx: SQLParser.Drop_indexContext):
        index_name = to_str(ctx.Identifier())
        return self.manager.drop_index(index_name)

    def visitAlter_add_index(self, ctx: SQLParser.Alter_add_indexContext):
        pass

    def visitAlter_drop_index(self, ctx: SQLParser.Alter_drop_indexContext):
        pass

    def visitAlter_table_add(self, ctx: SQLParser.Alter_table_addContext):
        pass

    def visitAlter_table_drop(self, ctx: SQLParser.Alter_table_dropContext):
        pass

    def visitAlter_table_change(self, ctx: SQLParser.Alter_table_changeContext):
        pass

    def visitAlter_table_rename(self, ctx: SQLParser.Alter_table_renameContext):
        pass

    def visitAlter_table_drop_pk(self, ctx: SQLParser.Alter_table_drop_pkContext):
        pass

    def visitAlter_table_drop_foreign_key(self, ctx: SQLParser.Alter_table_drop_foreign_keyContext):
        pass

    def visitAlter_table_add_pk(self, ctx: SQLParser.Alter_table_add_pkContext):
        pass

    def visitAlter_table_add_foreign_key(self, ctx: SQLParser.Alter_table_add_foreign_keyContext):
        pass
