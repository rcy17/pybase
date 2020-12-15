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
            return aggregate
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
        # 
        print("Tables:")
        for table in self.manager.show_tables():
            print(table)
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
                assert len(names) == 1
                name, = names
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
        table_name = to_str(ctx.getChild(1))
        self.manager.describe_table(table_name)
    
    def visitInsert_into_table(self, ctx: SQLParser.Insert_into_tableContext):
        table_name = to_str(ctx.getChild(2))
        value_lists = self.visitValue_lists(ctx.value_lists())
        for value_list in value_lists:
            self.manager.insert_record(table_name, value_list)

    def visitValue_lists(self, ctx: SQLParser.Value_listsContext):
        return tuple(self.visitValue_list(each) for each in ctx.value_list())
    
    def visitValue_list(self, ctx: SQLParser.Value_listContext):
        return tuple(to_str(each) for each in ctx.value())
    
    def visitSelect_table(self, ctx: SQLParser.Select_tableContext):
        # Only for debug
        table_name = to_str(ctx.getChild(3))
        self.manager.scan_record(table_name)