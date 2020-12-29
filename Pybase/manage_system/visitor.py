"""
Visitor for SystemManager

Date: 2020/11/30
"""
from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.sql_parser.SQLVisitor import SQLVisitor
from Pybase.utils.tools import to_str, to_int
from Pybase.meta_system.info import ColumnInfo, TableInfo
from .manager import SystemManger
from .result import QueryResult


class SystemVisitor(SQLVisitor):
    """Visitor to finish system and database SQL"""

    def __init__(self, manager=None):
        super().__init__()
        self.manager: SystemManger = manager

    def aggregateResult(self, aggregate, nextResult):
        if nextResult is None:
            return aggregate
        if aggregate is None:
            return nextResult
        if isinstance(nextResult, QueryResult):
            return nextResult
        if not isinstance(aggregate, tuple):
            aggregate = (aggregate,)
        return aggregate + (nextResult,)

    def visitSystem_statement(self, ctx: SQLParser.System_statementContext):
        return QueryResult('databases', tuple(self.manager.dbs))

    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        self.manager.create_db(to_str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        self.manager.drop_db(to_str(ctx.Identifier()))

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        self.manager.use_db(to_str(ctx.Identifier()))

    def visitShow_tables(self, ctx: SQLParser.Show_tablesContext):
        return QueryResult('tables', self.manager.show_tables())

    def visitCreate_table(self, ctx: SQLParser.Create_tableContext):
        columns = ctx.field_list().accept(self)
        table_name = to_str(ctx.Identifier())
        self.manager.create_table(TableInfo(table_name, columns))

    def visitDrop_table(self, ctx: SQLParser.Drop_tableContext):
        table_name = to_str(ctx.Identifier())
        self.manager.drop_table(table_name)

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
                                                  size=size,
                                                  is_index=name == primary_key,
                                                  foreign=foreign_keys.get(name))
            elif isinstance(field, SQLParser.Foreign_key_fieldContext):

                field_name, table_name, refer_name = tuple(to_str(each) for each in field.Identifier())
                foreign_keys[field_name] = table_name, refer_name
            else:
                assert isinstance(field, SQLParser.Primary_key_fieldContext)
                names = field.identifiers().accept(self)
                # assert len(names) == 1
                name = names[0]
                assert name in name_to_column
                primary_key = name
        return list(name_to_column.values())

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

    def visitValue_lists(self, ctx: SQLParser.Value_listsContext):
        return tuple(each.accept(self) for each in ctx.value_list())

    def visitValue_list(self, ctx: SQLParser.Value_listContext):
        return tuple(to_str(each) for each in ctx.value())

    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        # Only for debug
        table_name_list: list = ctx.identifiers().accept(self)
        # self.manager.scan_record(table_name_list[0])
        conditions = ctx.where_and_clause().accept(self)
        result_map = {}
        compare_map = {}
        for table_name in table_name_list:
            result_map[table_name] = self.manager.cond_scan(table_name, conditions)
            compare_map[table_name] = self.manager.cond_scan_index(table_name, conditions)
        if len(table_name_list) == 1:
            self.manager.print_results(result_map[table_name_list[0]])
            self.manager.print_results(compare_map[table_name_list[0]])
        else:
            for table_name in table_name_list:
                self.manager.print_results(result_map[table_name])
            result = self.manager.cond_join(result_map, conditions)
            self.manager.print_results(result)
    
    def visitDelete_from_table(self, ctx: SQLParser.Delete_from_tableContext):
        table_name = to_str(ctx.Identifier())
        conditions = ctx.where_and_clause().accept(self)
        self.manager.delete_records(table_name, conditions)
    
    def visitUpdate_table(self, ctx: SQLParser.Update_tableContext):
        table_name = to_str(ctx.Identifier())
        conditions = ctx.where_and_clause().accept(self)
        set_value_map = ctx.set_clause().accept(self)
        self.manager.update_records(table_name, conditions, set_value_map)
    
    def visitWhere_and_clause(self, ctx: SQLParser.Where_and_clauseContext):
        return tuple(each.accept(self) for each in ctx.where_clause())

    def visitWhere_clause(self, ctx: SQLParser.Where_clauseContext):
        tbname, colname = ctx.column().accept(self)
        oper = to_str(ctx.operator())
        if oper == '=':
            oper = '=='
        val = ctx.expression().accept(self)
        if isinstance(val, tuple):
            return (tbname, colname, oper, val[0], val[1])
        else:
            return (tbname, colname, oper, val)
        
    
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
        self.manager.drop_index(index_name)
