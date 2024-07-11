package ai.rapids.cudf.schema;

import ai.rapids.cudf.Schema;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Interface for visiting a schema in post order.
 */
public interface SchemaVisitor<T, R> {
    R visitTopSchema(Schema schema, List<T> children);

    T visitStruct(Schema structType, List<T> children);

    T preVisitList(Schema listType);

    T visitList(Schema listType, T preVisitResult, T childResult);

    T visit(Schema primitiveType);


}
