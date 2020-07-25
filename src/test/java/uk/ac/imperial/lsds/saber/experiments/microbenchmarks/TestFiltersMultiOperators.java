package uk.ac.imperial.lsds.saber.experiments.microbenchmarks;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.Utils;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;

public class TestFiltersMultiOperators {

  public static final String usage = "usage: TestFilterQuery"
      + " [ --batch-size ]"
      + " [ --window-type ]"
      + " [ --window-size ]"
      + " [ --window-slide ]"
      + " [ --input-attributes ]"
      + " [ --comparisons ]"
      + " [ --selectivity ]"
      + " [ --tuples-per-insert ]";

  public static void main (String [] args) {

    try {

      /* Application-specific arguments */

      int batchSize = 1048576;

      WindowType windowType = WindowType.ROW_BASED;

      int windowSize = 1;
      int windowSlide = 1;

      int numberOfAttributes = 6;

      /* Either one of the other must be set
       *
       * If selectivity is greater than 0,
       * then we ignore comparisons
       */
      int numberOfComparisons = 0;
      int selectivity = 100;

      int tuplesPerInsert = 32768;

      /* Parse application-specific command-line arguments */

      int i, j;
      for (i = 0; i < args.length; ) {
        if ((j = i + 1) == args.length) {
          System.err.println(usage);
          System.exit(1);
        }
        if (args[i].equals("--batch-size")) {
          batchSize = Integer.parseInt(args[j]);
        } else
        if (args[i].equals("--window-type")) {
          windowType = WindowType.fromString(args[j]);
        } else
        if (args[i].equals("--window-size")) {
          windowSize = Integer.parseInt(args[j]);
        } else
        if (args[i].equals("--window-slide")) {
          windowSlide = Integer.parseInt(args[j]);
        } else
        if (args[i].equals("--input-attributes")) {
          numberOfAttributes = Integer.parseInt(args[j]);
        } else
        if (args[i].equals("--comparisons")) {
          numberOfComparisons = Integer.parseInt(args[j]);
        } else
        if (args[i].equals("--selectivity")) {
          selectivity = Integer.parseInt(args[j]);
        } else
        if (args[i].equals("--tuples-per-insert")) {
          tuplesPerInsert = Integer.parseInt(args[j]);
        } else
        if (! SystemConf.parse(args[i], args[j])) {
          System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
          System.exit(1);
        }
        i = j + 1;
      }

      SystemConf.dump();

      QueryConf queryConf = new QueryConf (batchSize);

      WindowDefinition window = new WindowDefinition (windowType, windowSize, windowSlide);

      /* Setup input stream schema:
       *
       * The first attribute is the timestamp, followed
       * by `numberOfAttributes` integer attributes.
       */

      int [] offsets = new int [numberOfAttributes + 1];

      offsets[0] = 0;
      int tupleSize = 8;

      for (i = 1; i < numberOfAttributes + 1; i++) {
        offsets[i] = tupleSize;
        tupleSize += 4;
      }

      ITupleSchema schema = new TupleSchema(offsets, tupleSize);

      schema.setAttributeType(0, PrimitiveType.LONG);

      for (i = 1; i < numberOfAttributes + 1; i++) {
        schema.setAttributeType(i, PrimitiveType.INT);
      }

      /* tupleSize equals schema.getTupleSize() */
      tupleSize = schema.getTupleSize();


      /* Default query is:
       *
       * SELECT *
       * FROM S1
       * WHERE S1.1 < 50 AND S1.2 = 1 AND S1.3 >
       */

      /* Build the first selection operator */
      IPredicate predicate = null;
      IPredicate [] predicates = new IPredicate [4];

      predicates[0] =
          new IntComparisonPredicate(IntComparisonPredicate.LESS_OP, new IntColumnReference(1), new IntConstant(128 / 2));

      predicates[1] =
          new IntComparisonPredicate(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(2), new IntConstant(1));

      predicates[2] =
          new IntComparisonPredicate(IntComparisonPredicate.GREATER_OP, new IntColumnReference(3), new IntConstant(128 / 4));

      predicates[3] =
          new IntComparisonPredicate(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(3), new IntConstant(128 / 4));

      predicate = new ANDPredicate(predicates);

      StringBuilder customPredicate = new StringBuilder ();
      customPredicate.append("int value = 1;\n");
      customPredicate.append("int attribute_value1 = __bswap32(p->tuple._1);\n");
      customPredicate.append("int attribute_value2 = __bswap32(p->tuple._2);\n");
      customPredicate.append("int attribute_value3 = __bswap32(p->tuple._3);\n");
      customPredicate.append("value = value & (attribute_value1 > 128 / 2) & ");
      customPredicate.append("(attribute_value2 == 1) & ");
      customPredicate.append("(attribute_value3 < 128 / 4) & ");
      customPredicate.append("(attribute_value3 == 128 / 4);\n");
      customPredicate.append("return value;\n");

      IOperatorCode cpuCode = new Selection (predicate);
      IOperatorCode gpuCode = new SelectionKernel (schema, predicate, customPredicate.toString(), batchSize);

      QueryOperator operator;
      operator = new QueryOperator (cpuCode, gpuCode);

      /* Produce the query */
      Set<QueryOperator> operators = new HashSet<QueryOperator>();
      operators.add(operator);

      long timestampReference = System.nanoTime();

      Query query = new Query (0, operators, schema, window, null, null, queryConf, timestampReference);

      Set<Query> queries = new HashSet<Query>();
      queries.add(query);

      QueryApplication application = new QueryApplication(queries);

      application.setup();


      /* Set up the input stream */

      byte [] data = new byte [tupleSize * tuplesPerInsert]; // The array size defines the buffer size.

      ByteBuffer b = ByteBuffer.wrap (data);


      /* Fill the buffer */
      //
      // 0:1, 1:[0, 100), 2:[0, 1], 3:[0, 100), 4:1, 5:1
      //
      int value = 0;
      Boolean flipper = false;
      while (b.hasRemaining()) {
        // #0
        b.putLong (1);

        // #1
        b.putInt(value);

        // #2
        if (flipper) {
          b.putInt(0);
        } else {
          b.putInt(1);
        }
        flipper = !flipper;

        // #3
        b.putInt(value);

        // #4 #5 #6
        for (i = 4; i < numberOfAttributes+1; i ++)
          b.putInt(1);

        value = (value + 1) % 128;
      }

      if (SystemConf.LATENCY_ON) {
        /* Reset timestamp */
        long systemTimestamp = (System.nanoTime() - timestampReference) / 1000L; /* us */
        long packedTimestamp = Utils.pack(systemTimestamp, b.getLong(0));
        b.putLong(0, packedTimestamp);
      }

      while (true) {

        application.processData (data);

        if (SystemConf.LATENCY_ON)
          b.putLong(0, Utils.pack((long) ((System.nanoTime() - timestampReference) / 1000L), 1L));
      }

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
