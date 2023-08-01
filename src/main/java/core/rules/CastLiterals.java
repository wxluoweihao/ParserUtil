package core.rules;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.*;

import java.util.Optional;

/**
 * Try to remove cast operation on identifier
 *
 * author: Weihao
 * Date: 2023-06-28
 */
public class CastLiterals extends AstVisitor<Node, Node> {

    private Query query;
    private QuerySpecification querySpecification;
    
    @Override
    protected Node visitNode(Node node, Node context) {
        if(node instanceof Query) {
            this.query = (Query) node;
            return this.visitQueryBody(this.query.getQueryBody(), context);
        }
        else if (node instanceof ComparisonExpression) {
            ComparisonExpression comparisonExpression = (ComparisonExpression) node;
            return this.visitComparisonExpression(comparisonExpression, context);
        }
        else {
            return null;
        }
    }


    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, Node context) {
        ComparisonExpression.Operator operator = node.getOperator();
        Expression leftExpr = node.getLeft();
        Expression rightExpr = node.getRight();
        Expression[] newExpressions = this.pushCastToRight(leftExpr, rightExpr);
        ComparisonExpression comparisonExpression = new ComparisonExpression(operator, newExpressions[0], newExpressions[1]);
        return comparisonExpression;
    }

    @Override
    protected Statement visitQuerySpecification(QuerySpecification node, Node context) {
        Optional<Expression> where = node.getWhere();
        if(where.isPresent()) {
            Expression whereExpr = where.get();
            if (whereExpr instanceof ComparisonExpression) {
                ComparisonExpression comparisonExpr= (ComparisonExpression) whereExpr;
                ComparisonExpression newComparisonExpr = (ComparisonExpression)this.visitComparisonExpression(comparisonExpr, context);
                this.querySpecification = new QuerySpecification(
                        node.getLocation().get(),
                        node.getSelect(),
                        node.getFrom(),
                        Optional.of(newComparisonExpr),
                        node.getGroupBy(),
                        node.getHaving(),
                        node.getWindows(),
                        node.getOrderBy(),
                        node.getOffset(),
                        node.getLimit()
                );

                return new Query(
                        this.querySpecification.getLocation().get(),
                        this.query.getWith(),
                        this.querySpecification,
                        this.query.getOrderBy(),
                        this.query.getOffset(),
                        this.query.getLimit()
                );
            }
        }
        return this.query;
    }

    @Override
    protected Node visitQueryBody(QueryBody node, Node context) {
        if(node instanceof QuerySpecification) {
            this.querySpecification = (QuerySpecification) node;
            return this.visitQuerySpecification(this.querySpecification, context);
        } else {
            return null;
        }
    }

    private boolean isLiteral(Expression expr) {
        if (expr instanceof Literal) {
            return true;
        }
        return false;
    }

    private boolean isCastIdentifier(Expression expr) {
        if (expr instanceof Cast) {
            Cast castExpr = (Cast) expr;
            if(castExpr.getExpression() instanceof Identifier) {
                return true;
            }
        }
        return false;
    }

    private Expression[] pushCastToRight(Expression leftExpr, Expression rightExpr) {
        Expression[] newExpressions = new Expression[] {leftExpr, rightExpr};
        if(isCastIdentifier(leftExpr) && isLiteral(rightExpr)) {
            Cast cast = (Cast) leftExpr;
            Expression leftOperand = cast.getExpression();

            if(leftOperand instanceof Identifier) {
                System.out.println("############## hahahhq");
                newExpressions[0] = leftOperand;
                newExpressions[1] = new Cast(rightExpr, new GenericDataType(cast.getLocation(), new Identifier("VARCHAR"), ImmutableList.of()));
            }

        }
        else if(isCastIdentifier(rightExpr) && isLiteral(leftExpr)) {
            Cast cast = (Cast) rightExpr;
            Expression leftOperand = cast.getExpression();

            if(leftOperand instanceof Identifier) {
                newExpressions[0] = leftOperand;
                newExpressions[1] = new Cast(leftExpr, new GenericDataType(cast.getLocation(), new Identifier("VARCHAR"), ImmutableList.of()));
            }
        }

        return newExpressions;
    }
}
