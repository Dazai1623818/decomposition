package decomposition.pipeline;

import decomposition.PartitionEvaluation;
import decomposition.cpq.CPQExpression;
import decomposition.model.Partition;
import java.util.List;

/** Per-partition synthesis outcome returned by {@link CpqSynthesizer}. */
public record PartitionSynthesisResult(
    List<Partition> validPartitions,
    List<CPQExpression> catalogue,
    PartitionEvaluation evaluation) {}
