"""
Here defines SystemManger class

Date: 2020/11/30
"""
from antlr4 import (FileStream, CommonTokenStream, BailErrorStrategy)

from Pybase.sql_parser.SQLLexer import SQLLexer
from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.sql_parser.SQLVisitor import SQLVisitor
from Pybase.record_system.manager import RecordManager


# from Pybase.index_system.


class SystemManger:
    """Class to manage the whole system"""

    class SystemVisitor(SQLVisitor):
        def visitSystem_statement(self, ctx: SQLParser.System_statementContext):
            pass

        def visitDb_statement(self, ctx: SQLParser.Db_statementContext):
            pass

    def __init__(self, base_path):
        self._RM = RecordManager()
        self._base_path = base_path
        self.current_db = None
        self.visitor = SystemManger.SystemVisitor()

    def execute(self, filename):
        input_stream = FileStream(filename)
        lexer = SQLLexer(input_stream)
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        # parser._errHandler = BailErrorStrategy()
        tree = parser.program()
        self.visitor.visit(tree)
