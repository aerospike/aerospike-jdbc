package com.aerospike.jdbc.query;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.OrderByExpression;
import com.aerospike.jdbc.model.QueryType;
import com.aerospike.jdbc.util.IOUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.tree.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public final class AerospikeQueryParser {

    private AerospikeQueryParser() {
    }

    public static AerospikeQuery parseSql(Node root) {
        AerospikeQuery query = new AerospikeQuery();
        (new QueryParser(query)).process(root, 0);
        return query;
    }

    static String formatName(QualifiedName name) {
        return name.getOriginalParts().stream().map(AerospikeExpressionParser::formatExpression)
                .collect(Collectors.joining("."));
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns) {
        if (columns != null && !columns.isEmpty()) {
            String formattedColumns = columns.stream().map(AerospikeExpressionParser::formatExpression)
                    .collect(Collectors.joining(", "));
            builder.append(" (").append(formattedColumns).append(')');
        }
    }

    private static class QueryParser extends AstVisitor<Void, Integer> {
        private final AerospikeQuery query;

        public QueryParser(AerospikeQuery query) {
            this.query = query;
        }

        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        protected Void visitExpression(Expression node, Integer indent) {
            Preconditions.checkArgument(indent == 0, "visitExpression should only be called at root");
            ExpressionFormatter.formatExpression(node);
            return null;
        }

        protected Void visitUnnest(Unnest node, Integer indent) {
            throw new UnsupportedOperationException("visitUnnest");
        }

        protected Void visitLateral(Lateral node, Integer indent) {
            throw new UnsupportedOperationException("visitLateral");
        }

        protected Void visitPrepare(Prepare node, Integer indent) {
            throw new UnsupportedOperationException("visitPrepare");
        }

        protected Void visitDeallocate(Deallocate node, Integer indent) {
            throw new UnsupportedOperationException("visitDeallocate");
        }

        protected Void visitExecute(Execute node, Integer indent) {
            throw new UnsupportedOperationException("visitExecute");
        }

        protected Void visitDescribeOutput(DescribeOutput node, Integer indent) {
            throw new UnsupportedOperationException("visitDescribeOutput");
        }

        protected Void visitDescribeInput(DescribeInput node, Integer indent) {
            throw new UnsupportedOperationException("visitDescribeInput");
        }

        protected Void visitQuery(Query node, Integer indent) {
            if (node.getWith().isPresent()) {
                throw new UnsupportedOperationException("WITH");
            }

            this.processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                this.process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                this.process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                this.process(node.getLimit().get(), indent);
            }

            return null;
        }

        private void processRelation(Relation relation, Integer indent) {
            if (relation instanceof Table) {
                query.setTable(((Table) relation).getName().toString());
            } else {
                this.process(relation, indent);
            }
        }

        protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
            this.process(node.getSelect(), indent);
            if (node.getFrom().isPresent()) {
                this.process(node.getFrom().get(), indent);
            }

            if (node.getWhere().isPresent()) {
                query.setWhere(AerospikeWhereParser.parseExpression(node.getWhere().get()));
            }

            if (node.getGroupBy().isPresent()) {
                throw new UnsupportedOperationException("GROUP BY");
            }

            if (node.getHaving().isPresent()) {
                throw new UnsupportedOperationException("HAVING");
            }

            if (node.getOrderBy().isPresent()) {
                this.process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                this.process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                this.process(node.getLimit().get(), indent);
            }

            return null;
        }

        protected Void visitOrderBy(OrderBy node, Integer indent) {
            node.getSortItems().forEach(input -> {
                OrderByExpression orderBy = new OrderByExpression();
                orderBy.setColumns(Arrays.asList(AerospikeExpressionParser.formatExpression(input.getSortKey())
                        .split(",").clone()));
                switch (input.getOrdering()) {
                    case ASCENDING:
                        orderBy.setOrdering(OrderByExpression.Ordering.ASC);
                        break;
                    case DESCENDING:
                        orderBy.setOrdering(OrderByExpression.Ordering.DESC);
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
                }
                query.appendOrderBy(orderBy);
            });
            return null;
        }

        protected Void visitOffset(Offset node, Integer indent) {
            query.setOffset(Integer.parseInt(AerospikeExpressionParser.formatExpression(node.getRowCount())));
            return null;
        }

        protected Void visitFetchFirst(FetchFirst node, Integer indent) {
            throw new UnsupportedOperationException("visitFetchFirst");
        }

        protected Void visitLimit(Limit node, Integer indent) {
            query.setLimit(Integer.parseInt(AerospikeExpressionParser.formatExpression(node.getRowCount())));
            return null;
        }

        protected Void visitSelect(Select node, Integer indent) {
            query.setType(QueryType.SELECT);
            if (node.isDistinct()) {
                query.setDistinct(true);
            }

            if (node.getSelectItems().size() > 1) {
                for (SelectItem item : node.getSelectItems()) {
                    query.appendColumns(indentString(indent));
                    this.process(item, indent);
                }
            } else {
                this.process(Iterables.getOnlyElement(node.getSelectItems()), indent);
            }

            return null;
        }

        protected Void visitSingleColumn(SingleColumn node, Integer indent) {
            query.appendColumns(ExpressionFormatter.formatExpression(node.getExpression()));
            return null;
        }

        protected Void visitAllColumns(AllColumns node, Integer context) {
            node.getTarget().ifPresent((value) -> query.setTable(ExpressionFormatter.formatExpression(value)));
            query.appendColumns("*");
            return null;
        }

        protected Void visitTable(Table node, Integer indent) {
            query.setTable(AerospikeQueryParser.formatName(node.getName()));
            return null;
        }

        protected Void visitJoin(Join node, Integer indent) {
            throw new UnsupportedOperationException("visitJoin");
        }

        protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            throw new UnsupportedOperationException("visitAliasedRelation");
        }

        protected Void visitSampledRelation(SampledRelation node, Integer indent) {
            throw new UnsupportedOperationException("visitSampledRelation");
        }

        protected Void visitValues(Values node, Integer indent) {
            boolean first = true;
            StringBuilder builder = new StringBuilder();
            for (Iterator<Expression> var4 = node.getRows().iterator(); var4.hasNext(); first = false) {
                Expression row = var4.next();
                builder.append(indentString(indent)).append(first ? "  " : ", ");
                builder.append(AerospikeExpressionParser.formatExpression(row));
            }

            query.setValues(Arrays.stream(builder.toString().split(",")).map(String::trim)
                    .collect(Collectors.toList()));
            return null;
        }

        protected Void visitTableSubquery(TableSubquery node, Integer indent) {
            throw new UnsupportedOperationException("visitTableSubquery");
        }

        protected Void visitUnion(Union node, Integer indent) {
            throw new UnsupportedOperationException("visitUnion");
        }

        protected Void visitExcept(Except node, Integer indent) {
            throw new UnsupportedOperationException("visitExcept");
        }

        protected Void visitIntersect(Intersect node, Integer indent) {
            throw new UnsupportedOperationException("visitIntersect");
        }

        protected Void visitCreateView(CreateView node, Integer indent) {
            throw new UnsupportedOperationException("visitCreateView");
        }

        protected Void visitRenameView(RenameView node, Integer context) {
            throw new UnsupportedOperationException("visitRenameView");
        }

        protected Void visitSetViewAuthorization(SetViewAuthorization node, Integer context) {
            throw new UnsupportedOperationException("visitSetViewAuthorization");
        }

        protected Void visitCreateMaterializedView(CreateMaterializedView node, Integer indent) {
            throw new UnsupportedOperationException("visitCreateMaterializedView");
        }

        protected Void visitRefreshMaterializedView(RefreshMaterializedView node, Integer context) {
            throw new UnsupportedOperationException("visitRefreshMaterializedView");
        }

        protected Void visitDropMaterializedView(DropMaterializedView node, Integer context) {
            throw new UnsupportedOperationException("visitDropMaterializedView");
        }

        protected Void visitDropView(DropView node, Integer context) {
            throw new UnsupportedOperationException("visitDropView");
        }

        protected Void visitExplain(Explain node, Integer indent) {
            throw new UnsupportedOperationException("visitExplain");
        }

        protected Void visitShowCatalogs(ShowCatalogs node, Integer context) {
            query.setType(QueryType.SHOW_CATALOGS);
            node.getLikePattern().ifPresent((value) -> query.setLike(AerospikeExpressionParser.formatStringLiteral(value)));
            node.getEscape().ifPresent((value) -> query.setEscape(AerospikeExpressionParser.formatStringLiteral(value)));
            return null;
        }

        protected Void visitShowSchemas(ShowSchemas node, Integer context) {
            query.setType(QueryType.SHOW_SCHEMAS);
            if (node.getCatalog().isPresent()) {
                query.setCatalog(node.getCatalog().get().toString());
            }
            node.getLikePattern().ifPresent((value) -> query.setLike(AerospikeExpressionParser.formatStringLiteral(value)));
            node.getEscape().ifPresent((value) -> query.setEscape(AerospikeExpressionParser.formatStringLiteral(value)));
            return null;
        }

        protected Void visitShowTables(ShowTables node, Integer context) {
            query.setType(QueryType.SHOW_TABLES);
            node.getSchema().ifPresent((value) -> query.setSchema(AerospikeQueryParser.formatName(value)));
            node.getLikePattern().ifPresent((value) -> query.setLike(AerospikeExpressionParser.formatStringLiteral(value)));
            node.getEscape().ifPresent((value) -> query.setEscape(AerospikeExpressionParser.formatStringLiteral(value)));
            return null;
        }

        protected Void visitShowCreate(ShowCreate node, Integer context) {
            throw new UnsupportedOperationException("visitShowCreate");
        }

        protected Void visitShowColumns(ShowColumns node, Integer context) {
            query.setType(QueryType.SHOW_COLUMNS);
            query.setTable(AerospikeQueryParser.formatName(node.getTable()));
            node.getLikePattern().ifPresent((value) -> query.setLike(AerospikeExpressionParser.formatStringLiteral(value)));
            node.getEscape().ifPresent((value) -> query.setEscape(AerospikeExpressionParser.formatStringLiteral(value)));
            return null;
        }

        protected Void visitShowStats(ShowStats node, Integer context) {
            throw new UnsupportedOperationException("visitShowStats");
        }

        protected Void visitShowFunctions(ShowFunctions node, Integer context) {
            throw new UnsupportedOperationException("visitShowFunctions");
        }

        protected Void visitShowSession(ShowSession node, Integer context) {
            throw new UnsupportedOperationException("visitShowSession");
        }

        protected Void visitDelete(Delete node, Integer context) {
            query.setType(QueryType.DELETE);
            query.setTable(AerospikeQueryParser.formatName(node.getTable().getName()));
            if (node.getWhere().isPresent()) {
                query.setWhere(AerospikeWhereParser.parseExpression(node.getWhere().get()));
            }
            return null;
        }

        protected Void visitCreateSchema(CreateSchema node, Integer context) {
            throw new UnsupportedOperationException("visitCreateSchema");
        }

        protected Void visitDropSchema(DropSchema node, Integer context) {
            query.setType(QueryType.DROP_SCHEMA);
            return null;
        }

        protected Void visitRenameSchema(RenameSchema node, Integer context) {
            throw new UnsupportedOperationException("visitRenameSchema");
        }

        protected Void visitSetSchemaAuthorization(SetSchemaAuthorization node, Integer context) {
            throw new UnsupportedOperationException("visitSetSchemaAuthorization");
        }

        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent) {
            throw new UnsupportedOperationException("visitCreateTableAsSelect");
        }

        protected Void visitCreateTable(CreateTable node, Integer indent) {
            throw new UnsupportedOperationException("visitCreateTable");
        }

        protected Void visitDropTable(DropTable node, Integer context) {
            query.setType(QueryType.DROP_TABLE);
            query.setTable(node.getTableName().toString());
            return null;
        }

        protected Void visitRenameTable(RenameTable node, Integer context) {
            throw new UnsupportedOperationException("visitRenameTable");
        }

        protected Void visitComment(Comment node, Integer context) {
            throw new UnsupportedOperationException("visitComment");
        }

        protected Void visitRenameColumn(RenameColumn node, Integer context) {
            throw new UnsupportedOperationException("visitRenameColumn");
        }

        protected Void visitDropColumn(DropColumn node, Integer context) {
            throw new UnsupportedOperationException("visitDropColumn");
        }

        protected Void visitAnalyze(Analyze node, Integer context) {
            throw new UnsupportedOperationException("visitAnalyze");
        }

        protected Void visitAddColumn(AddColumn node, Integer indent) {
            throw new UnsupportedOperationException("visitAddColumn");
        }

        protected Void visitSetTableAuthorization(SetTableAuthorization node, Integer context) {
            throw new UnsupportedOperationException("visitSetTableAuthorization");
        }

        protected Void visitInsert(Insert node, Integer indent) {
            query.setType(QueryType.INSERT);
            query.setTable(AerospikeQueryParser.formatName(node.getTarget()));
            if (node.getColumns().isPresent()) {
                query.setColumns(node.getColumns().get().stream().map(Object::toString).map(IOUtils::stripQuotes)
                        .collect(Collectors.toList()));
            }

            this.process(node.getQuery(), indent); // ?
            return null;
        }

        public Void visitSetSession(SetSession node, Integer context) {
            throw new UnsupportedOperationException("visitSetSession");
        }

        public Void visitResetSession(ResetSession node, Integer context) {
            throw new UnsupportedOperationException("visitResetSession");
        }

        protected Void visitCallArgument(CallArgument node, Integer indent) {
            throw new UnsupportedOperationException("visitCallArgument");
        }

        protected Void visitCall(Call node, Integer indent) {
            throw new UnsupportedOperationException("visitCall");
        }

        protected Void visitRow(Row node, Integer indent) {
            throw new UnsupportedOperationException("visitRow");
        }

        protected Void visitStartTransaction(StartTransaction node, Integer indent) {
            throw new UnsupportedOperationException("visitStartTransaction");
        }

        protected Void visitIsolationLevel(Isolation node, Integer indent) {
            throw new UnsupportedOperationException("visitIsolationLevel");
        }

        protected Void visitTransactionAccessMode(TransactionAccessMode node, Integer context) {
            throw new UnsupportedOperationException("visitTransactionAccessMode");
        }

        protected Void visitCommit(Commit node, Integer context) {
            throw new UnsupportedOperationException("visitCommit");
        }

        protected Void visitRollback(Rollback node, Integer context) {
            throw new UnsupportedOperationException("visitRollback");
        }

        protected Void visitCreateRole(CreateRole node, Integer context) {
            throw new UnsupportedOperationException("visitCreateRole");
        }

        protected Void visitDropRole(DropRole node, Integer context) {
            throw new UnsupportedOperationException("visitDropRole");
        }

        protected Void visitGrantRoles(GrantRoles node, Integer context) {
            throw new UnsupportedOperationException("visitGrantRoles");
        }

        protected Void visitRevokeRoles(RevokeRoles node, Integer context) {
            throw new UnsupportedOperationException("visitRevokeRoles");
        }

        protected Void visitSetRole(SetRole node, Integer context) {
            throw new UnsupportedOperationException("visitSetRole");
        }

        public Void visitGrant(Grant node, Integer indent) {
            throw new UnsupportedOperationException("visitGrant");
        }

        public Void visitRevoke(Revoke node, Integer indent) {
            throw new UnsupportedOperationException("visitRevoke");
        }

        public Void visitShowGrants(ShowGrants node, Integer indent) {
            throw new UnsupportedOperationException("visitShowGrants");
        }

        protected Void visitShowRoles(ShowRoles node, Integer context) {
            throw new UnsupportedOperationException("visitShowRoles");
        }

        protected Void visitShowRoleGrants(ShowRoleGrants node, Integer context) {
            throw new UnsupportedOperationException("visitShowRoleGrants");
        }

        public Void visitSetPath(SetPath node, Integer indent) {
            throw new UnsupportedOperationException("visitSetPath");
        }

        private static String indentString(int indent) {
            return Strings.repeat("   ", indent);
        }
    }
}
