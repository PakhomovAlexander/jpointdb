package io.jpointdb.core.sql;

import io.jpointdb.core.sql.SqlAst.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Recursive-descent parser for a SQL subset sufficient for ClickBench.
 *
 * <p>
 * Precedence (lowest → highest): OR, AND, NOT, comparison (= &lt;&gt; &lt;
 * &lt;= &gt; &gt;=, IS NULL, LIKE, IN, BETWEEN), additive, multiplicative,
 * unary minus, primary.
 */
public final class SqlParser {

    private final Tokenizer tokenizer;

    private SqlParser(String sql) {
        this.tokenizer = new Tokenizer(sql);
    }

    public static Select parseSelect(String sql) {
        SqlParser p = new SqlParser(sql);
        Select s = p.parseSelectStmt();
        if (p.peek().type() == TokenType.SEMICOLON)
            p.next();
        p.expect(TokenType.EOF);
        return s;
    }

    // ---------- SELECT ----------

    private Select parseSelectStmt() {
        expect(TokenType.SELECT);
        List<SelectItem> items = parseSelectList();
        expect(TokenType.FROM);
        String fromTable = expectIdent();
        Expr where = null;
        if (peek().type() == TokenType.WHERE) {
            next();
            where = parseExpr();
        }
        List<Expr> groupBy = List.of();
        if (peek().type() == TokenType.GROUP) {
            next();
            expect(TokenType.BY);
            groupBy = parseExprList();
        }
        Expr having = null;
        if (peek().type() == TokenType.HAVING) {
            next();
            having = parseExpr();
        }
        List<OrderByItem> orderBy = List.of();
        if (peek().type() == TokenType.ORDER) {
            next();
            expect(TokenType.BY);
            orderBy = parseOrderByList();
        }
        Long limit = null;
        Long offset = null;
        if (peek().type() == TokenType.LIMIT) {
            next();
            limit = expectIntLiteral();
            if (peek().type() == TokenType.OFFSET) {
                next();
                offset = expectIntLiteral();
            }
        }
        return new Select(items, fromTable, where, groupBy, having, orderBy, limit, offset);
    }

    private List<SelectItem> parseSelectList() {
        List<SelectItem> items = new ArrayList<>();
        items.add(parseSelectItem());
        while (peek().type() == TokenType.COMMA) {
            next();
            items.add(parseSelectItem());
        }
        return items;
    }

    private SelectItem parseSelectItem() {
        if (peek().type() == TokenType.STAR) {
            next();
            return new SelectItem(Star.INSTANCE, null);
        }
        Expr expr = parseExpr();
        String alias = null;
        if (peek().type() == TokenType.AS) {
            next();
            alias = expectIdent();
        } else if (peek().type() == TokenType.IDENT && !isReservedContext(peek())) {
            alias = next().text();
        }
        return new SelectItem(expr, alias);
    }

    private boolean isReservedContext(Token t) {
        // Conservatively allow bare identifier as alias only when the next token
        // isn't beginning of a SQL continuation. We already handle keywords at
        // the tokenizer level, so this just ensures the identifier is free-standing.
        return false;
    }

    private List<Expr> parseExprList() {
        List<Expr> out = new ArrayList<>();
        out.add(parseExpr());
        while (peek().type() == TokenType.COMMA) {
            next();
            out.add(parseExpr());
        }
        return out;
    }

    private List<OrderByItem> parseOrderByList() {
        List<OrderByItem> items = new ArrayList<>();
        items.add(parseOrderByItem());
        while (peek().type() == TokenType.COMMA) {
            next();
            items.add(parseOrderByItem());
        }
        return items;
    }

    private OrderByItem parseOrderByItem() {
        Expr e = parseExpr();
        SortDirection dir = SortDirection.ASC;
        if (peek().type() == TokenType.ASC) {
            next();
            dir = SortDirection.ASC;
        } else if (peek().type() == TokenType.DESC) {
            next();
            dir = SortDirection.DESC;
        }
        return new OrderByItem(e, dir);
    }

    // ---------- Expressions ----------

    Expr parseExpr() {
        return parseOr();
    }

    private Expr parseOr() {
        Expr left = parseAnd();
        while (peek().type() == TokenType.OR) {
            next();
            Expr right = parseAnd();
            left = new Binary(BinaryOp.OR, left, right);
        }
        return left;
    }

    private Expr parseAnd() {
        Expr left = parseNot();
        while (peek().type() == TokenType.AND) {
            next();
            Expr right = parseNot();
            left = new Binary(BinaryOp.AND, left, right);
        }
        return left;
    }

    private Expr parseNot() {
        if (peek().type() == TokenType.NOT) {
            next();
            return new Unary(UnaryOp.NOT, parseNot());
        }
        return parseComparison();
    }

    private Expr parseComparison() {
        Expr left = parseAdditive();
        while (true) {
            Token t = peek();
            switch (t.type()) {
                case EQ -> {
                    next();
                    left = new Binary(BinaryOp.EQ, left, parseAdditive());
                }
                case NEQ -> {
                    next();
                    left = new Binary(BinaryOp.NEQ, left, parseAdditive());
                }
                case LT -> {
                    next();
                    left = new Binary(BinaryOp.LT, left, parseAdditive());
                }
                case LE -> {
                    next();
                    left = new Binary(BinaryOp.LE, left, parseAdditive());
                }
                case GT -> {
                    next();
                    left = new Binary(BinaryOp.GT, left, parseAdditive());
                }
                case GE -> {
                    next();
                    left = new Binary(BinaryOp.GE, left, parseAdditive());
                }
                case IS -> {
                    next();
                    boolean negated = false;
                    if (peek().type() == TokenType.NOT) {
                        next();
                        negated = true;
                    }
                    expect(TokenType.NULL);
                    left = new IsNull(left, negated);
                }
                case LIKE -> {
                    next();
                    left = new Like(left, parseAdditive(), false);
                }
                case NOT -> {
                    // Peek ahead: only NOT LIKE / NOT IN / NOT BETWEEN bind here.
                    Token saved = next();
                    switch (peek().type()) {
                        case LIKE -> {
                            next();
                            left = new Like(left, parseAdditive(), true);
                        }
                        case IN -> {
                            next();
                            left = parseInList(left, true);
                        }
                        case BETWEEN -> {
                            next();
                            left = parseBetween(left, true);
                        }
                        default -> throw new SqlException("expected LIKE/IN/BETWEEN after NOT", saved.position());
                    }
                }
                case IN -> {
                    next();
                    left = parseInList(left, false);
                }
                case BETWEEN -> {
                    next();
                    left = parseBetween(left, false);
                }
                default -> {
                    return left;
                }
            }
        }
    }

    private Expr parseInList(Expr value, boolean negated) {
        expect(TokenType.LPAREN);
        List<Expr> items = parseExprList();
        expect(TokenType.RPAREN);
        return new InList(value, items, negated);
    }

    private Expr parseBetween(Expr value, boolean negated) {
        Expr low = parseAdditive();
        expect(TokenType.AND);
        Expr high = parseAdditive();
        Expr check =
                new Binary(BinaryOp.AND, new Binary(BinaryOp.GE, value, low), new Binary(BinaryOp.LE, value, high));
        return negated ? new Unary(UnaryOp.NOT, check) : check;
    }

    private Expr parseAdditive() {
        Expr left = parseMultiplicative();
        while (true) {
            Token t = peek();
            if (t.type() == TokenType.PLUS) {
                next();
                left = new Binary(BinaryOp.PLUS, left, parseMultiplicative());
            } else if (t.type() == TokenType.MINUS) {
                next();
                left = new Binary(BinaryOp.MINUS, left, parseMultiplicative());
            } else
                return left;
        }
    }

    private Expr parseMultiplicative() {
        Expr left = parseUnary();
        while (true) {
            Token t = peek();
            if (t.type() == TokenType.STAR) {
                next();
                left = new Binary(BinaryOp.MUL, left, parseUnary());
            } else if (t.type() == TokenType.SLASH) {
                next();
                left = new Binary(BinaryOp.DIV, left, parseUnary());
            } else if (t.type() == TokenType.PERCENT) {
                next();
                left = new Binary(BinaryOp.MOD, left, parseUnary());
            } else
                return left;
        }
    }

    private Expr parseUnary() {
        if (peek().type() == TokenType.MINUS) {
            next();
            return new Unary(UnaryOp.NEG, parseUnary());
        }
        if (peek().type() == TokenType.PLUS) {
            next();
            return parseUnary();
        }
        return parsePrimary();
    }

    private Expr parsePrimary() {
        Token t = peek();
        switch (t.type()) {
            case INT_LITERAL :
                next();
                return Literal.ofInt(t.asLong());
            case FLOAT_LITERAL :
                next();
                return Literal.ofFloat(t.asDouble());
            case STRING_LITERAL :
                next();
                return Literal.ofString(t.text());
            case TRUE :
                next();
                return Literal.ofBool(true);
            case FALSE :
                next();
                return Literal.ofBool(false);
            case NULL :
                next();
                return Literal.ofNull();
            case STAR :
                next();
                return Star.INSTANCE;
            case LPAREN : {
                next();
                Expr e = parseExpr();
                expect(TokenType.RPAREN);
                return e;
            }
            case CASE :
                return parseCase();
            case IDENT :
                return parseIdentOrCall();
            default :
                throw new SqlException("unexpected token " + t.type() + " ('" + t.text() + "')", t.position());
        }
    }

    private Expr parseCase() {
        expect(TokenType.CASE);
        List<WhenClause> whens = new ArrayList<>();
        while (peek().type() == TokenType.WHEN) {
            next();
            Expr whenExpr = parseExpr();
            expect(TokenType.THEN);
            Expr thenExpr = parseExpr();
            whens.add(new WhenClause(whenExpr, thenExpr));
        }
        Expr elseExpr = null;
        if (peek().type() == TokenType.ELSE) {
            next();
            elseExpr = parseExpr();
        }
        expect(TokenType.END);
        if (whens.isEmpty())
            throw new SqlException("CASE must have at least one WHEN", tokenizer.peek().position());
        return new CaseExpr(whens, elseExpr);
    }

    private Expr parseIdentOrCall() {
        Token nameTok = next();
        if (peek().type() == TokenType.LPAREN) {
            next();
            // Special form: EXTRACT(<field> FROM <expr>)
            if ("extract".equalsIgnoreCase(nameTok.text())) {
                Token fieldTok = next();
                if (fieldTok.type() != TokenType.IDENT) {
                    throw new SqlException("expected field name after EXTRACT(", fieldTok.position());
                }
                expect(TokenType.FROM);
                Expr arg = parseExpr();
                expect(TokenType.RPAREN);
                return new FunctionCall("extract",
                        List.of(Literal.ofString(fieldTok.text().toLowerCase(Locale.ROOT)), arg), false);
            }
            boolean distinct = false;
            if (peek().type() == TokenType.DISTINCT) {
                next();
                distinct = true;
            }
            List<Expr> args = new ArrayList<>();
            if (peek().type() != TokenType.RPAREN) {
                args.add(parseExpr());
                while (peek().type() == TokenType.COMMA) {
                    next();
                    args.add(parseExpr());
                }
            }
            expect(TokenType.RPAREN);
            return new FunctionCall(nameTok.text().toLowerCase(Locale.ROOT), args, distinct);
        }
        return new ColumnRef(nameTok.text());
    }

    // ---------- Helpers ----------

    private Token peek() {
        return tokenizer.peek();
    }

    private Token next() {
        return tokenizer.next();
    }

    private void expect(TokenType type) {
        Token t = next();
        if (t.type() != type) {
            throw new SqlException("expected " + type + " but got " + t.type() + " ('" + t.text() + "')", t.position());
        }
    }

    private String expectIdent() {
        Token t = next();
        if (t.type() != TokenType.IDENT) {
            throw new SqlException("expected identifier but got " + t.type() + " ('" + t.text() + "')", t.position());
        }
        return t.text();
    }

    private long expectIntLiteral() {
        Token t = next();
        if (t.type() != TokenType.INT_LITERAL) {
            throw new SqlException("expected integer literal but got " + t.type(), t.position());
        }
        return t.asLong();
    }
}
