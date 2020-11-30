# Generated from SQL.g4 by ANTLR 4.8
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SQLParser import SQLParser
else:
    from SQLParser import SQLParser

# This class defines a complete generic visitor for a parse tree produced by SQLParser.

class SQLVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by SQLParser#program.
    def visitProgram(self, ctx:SQLParser.ProgramContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#statement.
    def visitStatement(self, ctx:SQLParser.StatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#system_statement.
    def visitSystem_statement(self, ctx:SQLParser.System_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#create_db.
    def visitCreate_db(self, ctx:SQLParser.Create_dbContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#drop_db.
    def visitDrop_db(self, ctx:SQLParser.Drop_dbContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#use_db.
    def visitUse_db(self, ctx:SQLParser.Use_dbContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#show_tables.
    def visitShow_tables(self, ctx:SQLParser.Show_tablesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#table_statement.
    def visitTable_statement(self, ctx:SQLParser.Table_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#index_statement.
    def visitIndex_statement(self, ctx:SQLParser.Index_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#alter_statement.
    def visitAlter_statement(self, ctx:SQLParser.Alter_statementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#field_list.
    def visitField_list(self, ctx:SQLParser.Field_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#field.
    def visitField(self, ctx:SQLParser.FieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#type_.
    def visitType_(self, ctx:SQLParser.Type_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value_lists.
    def visitValue_lists(self, ctx:SQLParser.Value_listsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value_list.
    def visitValue_list(self, ctx:SQLParser.Value_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value.
    def visitValue(self, ctx:SQLParser.ValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#where_and_clause.
    def visitWhere_and_clause(self, ctx:SQLParser.Where_and_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#where_clause.
    def visitWhere_clause(self, ctx:SQLParser.Where_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#column.
    def visitColumn(self, ctx:SQLParser.ColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#expression.
    def visitExpression(self, ctx:SQLParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#set_clause.
    def visitSet_clause(self, ctx:SQLParser.Set_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#selector.
    def visitSelector(self, ctx:SQLParser.SelectorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#identifiers.
    def visitIdentifiers(self, ctx:SQLParser.IdentifiersContext):
        return self.visitChildren(ctx)



del SQLParser