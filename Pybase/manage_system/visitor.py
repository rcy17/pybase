"""
Visitor for SystemManager

Date: 2020/11/30
"""
from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.sql_parser.SQLVisitor import SQLVisitor
from Pybase.utils.tools import to_str
from .manager import SystemManger


class SystemVisitor(SQLVisitor):
    """Visitor to finish system and database SQL"""

    def __init__(self, manager=None):
        super().__init__()
        self.manager: SystemManger = manager

    def visitSystem_statement(self, ctx: SQLParser.System_statementContext):
        print(*self.manager.dbs, sep='\n')

    def visitCreate_db(self, ctx: SQLParser.Create_dbContext):
        self.manager.create_db(to_str(ctx.Identifier()))

    def visitDrop_db(self, ctx: SQLParser.Drop_dbContext):
        self.manager.drop_db(to_str(ctx.Identifier()))

    def visitUse_db(self, ctx: SQLParser.Use_dbContext):
        self.manager.use_db(to_str(ctx.Identifier()))

    def visitShow_tables(self, ctx: SQLParser.Show_tablesContext):
        self.manager.show_tables()
